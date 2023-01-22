cargo build 
sudo rm -f /usr/local/bin/fortunate
sudo cp /Users/louis/workspace/fortunate/fortunate/target/debug/fortunate /usr/local/bin/. 
RUST_BACKTRACE=1 RUST_LOG=INFO /usr/local/bin/fortunate $1 $2 $3
