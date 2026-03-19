CXX := g++
PYTHON := python3

TARGET := main
SRC_DIR := src
BUILD_DIR := build
RESULTS_DIR := results

MODE ?= release
METHODS ?=  fast 
PROFILE_METHODS ?= poseidon ola fab fast
RUN_SEED ?= 123
RUN_NUM_CARDS ?= 1
RUN_NUM_USERS ?= 1
RUN_REQUESTS_PER_USER ?= 1
ALWAYS_DUMP_LOGICAL_GRAPH ?= 1
ALWAYS_DUMP_RUNTIME_PLAN ?= 1

CPPFLAGS := -Iinclude
CXXFLAGS := -std=c++17 -Wall -Wextra -Wpedantic -MMD -MP

ifeq ($(ALWAYS_DUMP_LOGICAL_GRAPH),1)
	CPPFLAGS += -DKEYAWARE_ALWAYS_DUMP_LOGICAL_GRAPH=1
endif
ifeq ($(ALWAYS_DUMP_RUNTIME_PLAN),1)
	CPPFLAGS += -DKEYAWARE_ALWAYS_DUMP_RUNTIME_PLAN=1
endif

ifeq ($(MODE),debug)
	CXXFLAGS += -O0 -g
else
	CXXFLAGS += -O2
endif

SOURCES := $(shell find $(SRC_DIR) -type f -name "*.cpp")
OBJECTS := $(patsubst $(SRC_DIR)/%.cpp,$(BUILD_DIR)/%.o,$(SOURCES))
DEPS := $(OBJECTS:.o=.d)

.PHONY: all make clean run draw draw-profile

all: $(TARGET)

make: all

$(TARGET): $(OBJECTS) Makefile
	$(CXX) $(OBJECTS) -o $@

$(BUILD_DIR)/%.o: $(SRC_DIR)/%.cpp Makefile
	@mkdir -p $(dir $@)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -c $< -o $@

run: $(TARGET)
	@mkdir -p $(RESULTS_DIR)
	@rm -f "$(RESULTS_DIR)/metrics_auto.csv" "$(RESULTS_DIR)/run_auto.log"
	@set -eu; \
	for m in $(METHODS); do \
		echo "=== running $$m ==="; \
		rm -f "$(RESULTS_DIR)/metrics_$$m.csv" "$(RESULTS_DIR)/run_$$m.log"; \
		./$(TARGET) \
			--workload synthetic \
			--num-cards $(RUN_NUM_CARDS) \
			--disable-multi-card \
			--num-users $(RUN_NUM_USERS) \
			--requests-per-user $(RUN_REQUESTS_PER_USER) \
			--ks-method "$$m" \
			--seed $(RUN_SEED) \
			--csv-output "$(RESULTS_DIR)/metrics_$$m.csv" \
			2>&1 | tee "$(RESULTS_DIR)/run_$$m.log"; \
	done
	@echo "Run completed. CSV/LOG files are under $(RESULTS_DIR)/"

draw:
	@mkdir -p $(RESULTS_DIR)
	@$(PYTHON) scripts/draw_method_latency.py \
		--input-glob "$(RESULTS_DIR)/metrics_*.csv" \
		--metric mean_latency \
		--output "$(RESULTS_DIR)/latency_mean.svg"
	@$(PYTHON) scripts/draw_method_latency.py \
		--input-glob "$(RESULTS_DIR)/metrics_*.csv" \
		--metric p99_latency \
		--output "$(RESULTS_DIR)/latency_p99.svg"
	@set -eu; \
	for m in $(PROFILE_METHODS); do \
		echo "=== drawing $$m execution profile ==="; \
		$(PYTHON) scripts/plot_execution.py \
			--ks-method "$$m" \
			--input-log "$(RESULTS_DIR)/run_$$m.log" \
			--seed $(RUN_SEED) \
			--num-cards $(RUN_NUM_CARDS) \
			--num-users $(RUN_NUM_USERS) \
			--requests-per-user $(RUN_REQUESTS_PER_USER) \
			--output "$(RESULTS_DIR)/$${m}_execution_profile.png"; \
	done
	@echo "Charts generated:"
	@echo "  $(RESULTS_DIR)/latency_mean.svg"
	@echo "  $(RESULTS_DIR)/latency_p99.svg"
	@for m in $(PROFILE_METHODS); do \
		echo "  $(RESULTS_DIR)/$${m}_execution_profile.png"; \
	done

draw-profile: $(TARGET)
	@mkdir -p $(RESULTS_DIR)
	@set -eu; \
	for m in $(PROFILE_METHODS); do \
		echo "=== drawing $$m execution profile ==="; \
		$(PYTHON) scripts/plot_execution.py \
			--ks-method "$$m" \
			--input-log "$(RESULTS_DIR)/run_$$m.log" \
			--seed $(RUN_SEED) \
			--num-cards $(RUN_NUM_CARDS) \
			--num-users $(RUN_NUM_USERS) \
			--requests-per-user $(RUN_REQUESTS_PER_USER) \
			--output "$(RESULTS_DIR)/$${m}_execution_profile.png"; \
	done
	@echo "Execution profiles generated:"
	@for m in $(PROFILE_METHODS); do \
		echo "  $(RESULTS_DIR)/$${m}_execution_profile.png"; \
	done

clean:
	rm -rf $(BUILD_DIR) $(TARGET)
	rm -f $(RESULTS_DIR)/metrics_*.csv \
	      $(RESULTS_DIR)/run_*.log \
	      $(RESULTS_DIR)/latency_*.svg \
	      $(RESULTS_DIR)/*_execution_profile.png

-include $(DEPS)
