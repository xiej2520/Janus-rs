use std::collections::BTreeMap;
use std::io::Read;

use std::fs::File;
use std::io::Write;

use apps::get_app;

mod apps;
mod util;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    if std::env::args().len() < 3 {
        eprintln!("Usage: mrsequential mapreduce_app inputfiles...");
        return Err("1".into());
    }

    let (map_fn, reduce_fn) = get_app(std::env::args().nth(1).unwrap().as_str());

    let files: Vec<_> = std::env::args().skip(2).collect();

    // read each input, pass to map, accumulate in intermediate
    let mut intermediate = vec![];
    for filename in files {
        let mut file = File::open(&filename).map_err(|e| {
            eprintln!("cannot open {}", filename);
            e
        })?;
        let mut contents = String::new();
        file.read_to_string(&mut contents).map_err(|e| {
            eprintln!("cannot read {}", filename);
            e
        })?;
        let kva = map_fn(&filename, &contents);
        intermediate.extend(kva);
    }

    // group by key
    let mut grouped: BTreeMap<_, Vec<_>> = BTreeMap::new();
    for kva in intermediate {
        grouped.entry(kva.key).or_default().push(kva.value);
    }

    let mut outf = File::create("mr-out-0").map_err(|e| {
        eprintln!("cannot create mr-out-0");
        e
    })?;

    for kvas in grouped {
        // call reduce on each key in intermediate
        let str_values: Vec<_> = kvas.1.iter().map(|s| s.as_str()).collect();
        let output = reduce_fn(&kvas.0, str_values);

        // write reduce output to file
        writeln!(outf, "{} {}", kvas.0, output)?;
    }

    Ok(())
}
