# Janus-rs

## MapReduce

Run `src/main/test-mr.sh` to run all tests.

```Shell
cargo run --bin mrcoordinator ../rsrc/pg-being_ernest.txt ../rsrc/pg-dorian_gray.txt ../rsrc/pg-frankenstein.txt ../rsrc/pg-frankenstein.txt ../rsrc/pg-grimm.txt ../rsrc/pg-huckleberry_finn.txt ../rsrc/pg-metamorphosis.txt ../rsrc/pg-sherlock_holmes.txt ../rsrc/pg-tom_sawyer.txt

cargo run --bin mrworker wc
```
