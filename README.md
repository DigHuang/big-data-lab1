# How To Run This Project

## If you just clone it now

1. Turn to the directory src/main
2. Run the following command

### Step1 Linux

```bash
# Build wc.go
go build -buildmode=plugin ../mrapps/wc.go

# Run the coordinator
go run mrcoordinator.go pg*.txt

# Run the workers
go run mrworker.go wc.so

# Check the result
cat mr-out-* | sort | more
```

## If you have already run the project before

1. Clear the intermediate files
2. Run the command above

### Step2 Linux

```bash
# Clear the intermediate files
rm -rf mr-* wc.so
```

## If you want to meet the res to the professional version

### Step3 Linux

```bash
sort -V -k1,1 -k2,2 mr-out* | grep . > mr-wc-all
```
