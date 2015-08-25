extern crate mmap;
extern crate time;
extern crate timely;
extern crate getopts;

extern crate graph_map;
extern crate graph_layout;
extern crate radix_sort;

use timely::progress::timestamp::RootTimestamp;
use timely::progress::nested::Summary::Local;
use timely::dataflow::*;
use timely::dataflow::operators::*;
use timely::dataflow::channels::pact::{Pipeline, Exchange};
use timely::drain::DrainExt;

use graph_layout::layout::*;
use graph_layout::compression::*;

use graph_map::graph_map::GraphMMap;

use radix_sort::*;

fn main () {

    /*
        The plan here is to partition the rows of the matrix into #processes parts, and the columns
        into #threads parts. This could be generally #row_parts and #col_parts, but that requires
        more thinking about things.
    */

    let threads: u32 = ::std::env::args().nth(1).unwrap().parse().unwrap();
    let filename = ::std::env::args().nth(2).unwrap();
    let nodes: u32 = ::std::env::args().nth(3).unwrap().parse().unwrap();

    timely::execute_from_args(::std::env::args().skip(4), move |root| {

        let index = root.index() as u32;
        let peers = root.peers() as u32;
        let processes = (peers / threads) as u32;
        let dst_index = index % threads;

        let start = time::precise_time_s();
        let mut going = start;

        let mut counter = 0;
        let mut sorter = RadixSorter::new();
        let hilbert = Hilbert::new();
        let mut decoder = BytewiseCached::new();
        let mut compressed = vec![];

        // let nodes: u32 = 3_563_602_788 + 1;

        let local_srcs = (nodes / processes) + 1;
        let local_dsts = (nodes / threads) + 1;

        // degree is ignored at the moment.
        // mostly just a pain to put together.
        let deg = vec![1.0f32; local_srcs as usize];
        let mut src = vec![1.0f32; local_srcs as usize];
        let mut dst = vec![0.0f32; local_dsts as usize];


        let mut input = root.scoped(|builder| {

            let (input, edges) = builder.new_input::<(u32, u32)>();
            let (cycle, ranks) = builder.loop_variable::<(u32, f32)>(RootTimestamp::new(20), Local(1));

            let ranks = edges.binary_notify(&ranks,
                                Exchange::new(|x: &(u32,u32)| ((x.0 % processes) * threads + (x.1 % threads)) as u64),
                                Pipeline,   // data are partitioned as part of the broadcast step.
                                "hilbrank",
                                vec![RootTimestamp::new(0)],
                                move |input1, input2, output, notificator| {

                // receive incoming edges (should only be iter 0)
                while let Some((_index, data)) = input1.next() {
                    counter += data.len();
                    for &(src,dst) in data.iter() {
                        let shrunk = (src / processes, dst / threads);
                        sorter.push(hilbert.entangle(shrunk), &|&x| x);
                    }

                    // should be 1GB per worker
                    if counter > (1 << 27) {
                        let mut results = sorter.finish(&|&x| x);
                        let mut compressor = Compressor::with_capacity(counter);
                        for vec in results.iter_mut() {
                            for elem in vec.drain_temp() {
                                compressor.push(elem);
                            }
                        }
                        sorter.recycle(results);
                        compressed.push(compressor.done());
                        counter = 0;
                    }
                }

                if counter > 0 {
                    // round out any remaining edges
                    let mut results = sorter.finish(&|&x| x);
                    let mut compressor = Compressor::with_capacity(counter);
                    for vec in results.iter_mut() {
                        for elem in vec.drain_temp() {
                            compressor.push(elem);
                        }
                    }
                    compressed.push(compressor.done());
                    counter = 0;
                }

                // all inputs received for iter, commence multiplication
                while let Some((iter, _)) = notificator.next() {

                    let now = time::now();

                    if index == 0 { println!("{}:{}:{}.{} starting iteration {}", now.tm_hour, now.tm_min, now.tm_sec, now.tm_nsec, iter.inner); }

                    // if the very first iteration, prepare some stuff.
                    if iter.inner == 0 {
                        // now we would merge the compressed representations, if we want to.
                        // let's measure both doing this and not doing it. :)


                    }

                    // record some timings in order to estimate per-iteration times
                    // if iter.inner == 0  && index == 0 { println!("src: {}, dst: {}, edges: {}", src.len(), rev.len(), trn.len()); }
                    if iter.inner == 10 && index == 0 { going = time::precise_time_s(); }
                    if iter.inner == 20 && index == 0 { println!("average: {}", (time::precise_time_s() - going) / 10.0 ); }

                    // prepare src for transmitting to destinations
                    for s in 0..src.len() { src[s] = 0.15 + 0.85 * src[s] / deg[s] as f32; }
                    for d in 0..dst.len() { dst[d] = 0.0; }

                    // do the multiplication for each compressed sequence
                    for sequence in &compressed {
                        for element in sequence.decompress() {
                            let (s,d) = decoder.detangle(element);
                            dst[d as usize] += src[s as usize];
                        }
                    }

                    for s in 0..src.len() { src[s] = 0.0; }

                    // now we need to send some stuff to other threads.
                    // TODO : This could be done as blocks, as they mostly go to the same place.
                    let mut session = output.session(&iter);
                    for (index, &value) in dst.iter().enumerate() {
                        if value != 0.0 {
                            session.give((index as u32 * threads + dst_index, value));
                        }
                    }
                }

                // receive data from threads, accumulate in src
                while let Some((iter, data)) = input2.next() {
                    notificator.notify_at(&iter);
                    for &(node, rank) in data.iter() {
                        src[(node / processes) as usize] += rank;
                    }
                }
            });

            let mut aggregates = vec![0.0f32; (nodes / peers) as usize];
            // want to do aggregation so that aggregates are local to the folks who will need them.
            // that is, (node, rank) goes to some worker on machine#: name % processes
            let aggregated = ranks.unary_notify(
                Exchange::new(move |&(n,_): &(u32,f32)| (((n % processes) * threads) + ((n / processes) % threads)) as u64),
                "aggregation",
                vec![],
                move |input, output, notificator | {

                while let Some((time, data)) = input.next() {
                    notificator.notify_at(time);
                    for &(node, rank) in data.iter() {
                        aggregates[(node / peers) as usize] += rank;
                    }
                }

                while let Some((time, _)) = notificator.next() {
                    let mut session = output.session(&time);
                    for (node, &rank) in aggregates.iter().enumerate() {
                        if rank != 0.0 {
                            session.give((node as u32 * peers, rank));
                        }
                    }
                }
            });

            // data are now aggregated, and only need to be broadcast out to the intended recipients
            // ideally, none of this actually ends up hitting the network. also, it seems like it
            // shouldn't even require looking at `node`, on account of already putting all the data
            // on the right processes. We should be able to use (index / threads) * threads, I think.
            let mut exchanged = aggregated.exchange(move |&(node, _)| ((node % processes) * threads) as u64);
            for i in 1..threads {
                exchanged = aggregated.exchange(move |&(node, _)| (((node % processes) * threads) + i) as u64)
                                      .concat(&exchanged);
            }

            exchanged.connect_loop(cycle);

            input
        });

        // introduce edges into the computation;
        // allow mmaped file to drop
        {
            let graph = GraphMMap::new(&filename);
            for node in 0..graph.nodes() {
                if node as u32 % peers == index {
                    for dst in graph.edges(node) {
                        input.send((node as u32, *dst as u32));
                    }
                }
            }
        }
        input.close();
        while root.step() { }

        if index == 0 { println!("elapsed: {}", time::precise_time_s() - start); }
    });

}
