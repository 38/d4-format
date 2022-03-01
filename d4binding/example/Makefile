DETECTED_SO=$(shell readlink bin/libd4binding.so 2> /dev/null) 
DETECTED_SO_DIR=$(shell dirname $(DETECTED_SO) 2> /dev/null)
DETECTED_CONFIG=$(shell basename $(DETECTED_SO_DIR) 2> /dev/null)
CONFIG:=$(if $(DETECTED_CONFIG),$(DETECTED_CONFIG),debug)
BIND_BIN_DIR=../../target/$(CONFIG)
OBJS=read create read-interval error mean-depth multithread-read read-index

all: print_config $(OBJS:%=bin/%) bin/libd4binding.so

print_config:
	@if [ "$(CONFIG)" != "$(DETECTED_CONFIG)" ]; then rm -f bin/*; fi
	@echo $(CONFIG)

$(BIND_BIN_DIR)/libd4binding.so: 
	@echo "[CARGO] build"
	@if [ "$(CONFIG)" = "debug" ]; then cargo build --package=d4binding; else cargo build --package=d4binding --release; fi

bin/%: %.c $(BIND_BIN_DIR)/libd4binding.so
	@echo "[CC]\t$<"
	@mkdir -p bin
	@if [ "$(CONFIG)" = "debug" ]; then \
		$(CC) $< -o $@ -g -I ../include -L $(BIND_BIN_DIR) -ld4binding; \
	else \
		$(CC) $< -o $@ -g -O3 -I ../include -L $(BIND_BIN_DIR) -ld4binding; \
	fi

bin/%_cpp: %.cpp $(BIND_BIN_DIR)/libd4binding.so
	@echo "[CXX]\t$<"
	@mkdir -p bin
	@if [ "$(CONFIG)" = "debug" ]; then \
		$(CXX) $< -o $@ -g -I ../include -L $(BIND_BIN_DIR) -ld4binding; \
	else \
		$(CXX) $< -o $@ -g -O3 -I ../include -L $(BIND_BIN_DIR) -ld4binding; \
	fi

bin/libd4binding.so: $(BIND_BIN_DIR)/libd4binding.so
	@echo "[LN]\t$<"
	@rm -f $@
	@ln -s `readlink -f $<` $@

bin/sample.d4: data/genome.txt data/data.bedgraph
	cd ../.. && \
	cargo run -- create -R0-1 -g d4binding/example/data/genome.txt d4binding/example/data/data.bedgraph d4binding/example/bin/sample.d4

clean: 
	rm -f bin/*

.PHONY: clean print_config
