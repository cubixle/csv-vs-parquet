go build -o app main.go

hyperfine --warmup 1 "./app -type csv -amount 100000"

hyperfine --warmup 1 "./app -type parquet -amount 100000"
