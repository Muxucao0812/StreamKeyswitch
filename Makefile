CXX := g++
PYTHON := python3

TARGET := main
SRC_DIR := src
BUILD_DIR := build
RESULTS_DIR := results

MODE ?= release
ALL_METHODS := poseidon fab fast ola hera max_parallel digit_centric output_centric cinnamon_oa cinnamon_ib
# METHODS ?= $(ALL_METHODS)
METHODS ?= $(ALL_METHODS)
# PROFILE_METHODS ?= $(ALL_METHODS)
PROFILE_METHODS ?= $(ALL_METHODS)
RUN_SEED ?= 123
RUN_NUM_USERS ?= 1
RUN_REQUESTS_PER_USER ?= 1
RUN_SINGLE_NUM_CARDS ?= 1
RUN_MULTI_NUM_CARDS ?= 3
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
		method_tag="$$m"; \
		ks_method="$$m"; \
		ks_multi_board_mode="auto"; \
		num_cards="$(RUN_SINGLE_NUM_CARDS)"; \
		multi_card_flag="--disable-multi-card"; \
			case "$$m" in \
				cinnamon_output_aggregation|cinnamon_oa) \
					num_cards="$(RUN_MULTI_NUM_CARDS)"; \
					multi_card_flag="--enable-multi-card" ;; \
				cinnamon_input_broadcast|cinnamon_ib) \
					num_cards="$(RUN_MULTI_NUM_CARDS)"; \
					multi_card_flag="--enable-multi-card" ;; \
		esac; \
		echo "=== running $$method_tag ==="; \
		rm -f "$(RESULTS_DIR)/metrics_$$method_tag.csv" "$(RESULTS_DIR)/run_$$method_tag.log"; \
			./$(TARGET) \
				--workload synthetic \
				--num-cards "$$num_cards" \
				"$$multi_card_flag" \
				--num-users $(RUN_NUM_USERS) \
				--requests-per-user $(RUN_REQUESTS_PER_USER) \
				--ks-method "$$ks_method" \
				--ks-multi-board-mode "$$ks_multi_board_mode" \
				--seed $(RUN_SEED) \
				--csv-output "$(RESULTS_DIR)/metrics_$$method_tag.csv" \
			2>&1 | tee "$(RESULTS_DIR)/run_$$method_tag.log"; \
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
		method_tag="$$m"; \
		ks_method="$$m"; \
		num_cards="$(RUN_SINGLE_NUM_CARDS)"; \
		multi_card_flag="--disable-multi-card"; \
			case "$$m" in \
				cinnamon_output_aggregation|cinnamon_input_broadcast|cinnamon_oa|cinnamon_ib) \
					ks_method="cinnamon"; \
					num_cards="$(RUN_MULTI_NUM_CARDS)"; \
					multi_card_flag="--enable-multi-card" ;; \
		esac; \
		echo "=== drawing $$m execution profile ==="; \
			$(PYTHON) scripts/plot_execution.py \
				--ks-method "$$ks_method" \
				--input-log "$(RESULTS_DIR)/run_$$method_tag.log" \
				--seed $(RUN_SEED) \
				--num-cards "$$num_cards" \
				"$$multi_card_flag" \
				--num-users $(RUN_NUM_USERS) \
				--requests-per-user $(RUN_REQUESTS_PER_USER) \
				--output "$(RESULTS_DIR)/$${method_tag}_execution_profile.png"; \
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
		method_tag="$$m"; \
		ks_method="$$m"; \
		num_cards="$(RUN_SINGLE_NUM_CARDS)"; \
		multi_card_flag="--disable-multi-card"; \
			case "$$m" in \
				cinnamon_output_aggregation|cinnamon_input_broadcast|cinnamon_oa|cinnamon_ib) \
					ks_method="cinnamon"; \
					num_cards="$(RUN_MULTI_NUM_CARDS)"; \
					multi_card_flag="--enable-multi-card" ;; \
		esac; \
		echo "=== drawing $$m execution profile ==="; \
			$(PYTHON) scripts/plot_execution.py \
				--ks-method "$$ks_method" \
				--input-log "$(RESULTS_DIR)/run_$$method_tag.log" \
				--seed $(RUN_SEED) \
				--num-cards "$$num_cards" \
				"$$multi_card_flag" \
				--num-users $(RUN_NUM_USERS) \
				--requests-per-user $(RUN_REQUESTS_PER_USER) \
				--output "$(RESULTS_DIR)/$${method_tag}_execution_profile.png"; \
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
