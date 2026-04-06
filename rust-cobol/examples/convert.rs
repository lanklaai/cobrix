use std::{fs::File, io::Write};

use ebcdic::ebcdic::Ebcdic;

fn main() {
    let bytes = include_bytes!("../data/CUSTOMER.ebcdic");
    let mut buffer = bytes.to_vec();
    Ebcdic::ebcdic_to_ascii(bytes, &mut buffer, bytes.len(), false, true);
    let mut f = File::create("../data/CUSTOMER.txt").unwrap();
    f.write_all(&buffer).unwrap();
    f.flush().unwrap();
}
