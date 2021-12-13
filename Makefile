BUILD_DIR := $(CURDIR)/_build

REBAR := rebar3

CT_NODE_NAME = ct@127.0.0.1

compile:
	$(REBAR) do compile, dialyzer, xref

.PHONY: all
all: compile test

.PHONY: clean
clean: distclean

.PHONY: distclean
distclean:
	@rm -rf _build erl_crash.dump rebar3.crashdump rebar.lock

.PHONY: xref
xref:
	$(REBAR) xref

.PHONY: eunit
eunit: compile
	$(REBAR) eunit verbose=true

.PHONY: test
test: smoke-test ct-consistency ct-fault-tolerance cover

.PHONY: smoke-test
smoke-test:
	$(REBAR) do eunit, ct -v --readable=false --name $(CT_NODE_NAME)

.PHONY: ct-consistency
ct-consistency:
	$(REBAR) ct -v --readable=false --name $(CT_NODE_NAME) --suite mria_proper_suite

.PHONY: ct-fault-tolerance
ct-fault-tolerance:
	$(REBAR) ct -v --readable=false --name $(CT_NODE_NAME) --suite mria_fault_tolerance_suite

.PHONY: ct-suite
ct-suite: compile
ifneq ($(TESTCASE),)
	$(REBAR) ct -v --readable=false --name $(CT_NODE_NAME) --suite $(SUITE)  --case $(TESTCASE)
else
	$(REBAR) ct -v --readable=false --name $(CT_NODE_NAME) --suite $(SUITE)
endif

cover: | smoke-test ct-consistency ct-fault-tolerance
	$(REBAR) cover

.PHONY: coveralls
coveralls:
	@rebar3 as test coveralls send

.PHONY: dialyzer
dialyzer:
	$(REBAR) dialyzer

CUTTLEFISH_SCRIPT = _build/default/lib/cuttlefish/cuttlefish

$(CUTTLEFISH_SCRIPT):
	@${REBAR} get-deps
	@if [ ! -f cuttlefish ]; then make -C _build/default/lib/cuttlefish; fi

app.config: $(CUTTLEFISH_SCRIPT)
	$(verbose) $(CUTTLEFISH_SCRIPT) -l info -e etc/ -c etc/mria.conf.example -i priv/mria.schema -d data/

##########################################################################################
# Concuerror
##########################################################################################

CONCUERROR := $(BUILD_DIR)/Concuerror/bin/concuerror
CONCUERROR_RUN := $(CONCUERROR) \
	--treat_as_normal shutdown --treat_as_normal normal --treat_as_normal intentional \
	--treat_as_normal cvar_set --treat_as_normal cvar_stopped --treat_as_normal cvar_retry \
	-x code -x code_server -x error_handler \
	-pa $(BUILD_DIR)/concuerror+test/lib/snabbkaffe/ebin \
	-pa $(BUILD_DIR)/concuerror+test/lib/mria/ebin

concuerror = $(CONCUERROR_RUN) -f $(BUILD_DIR)/concuerror+test/lib/mria/test/concuerror_tests.beam -t $(1) || \
	{ cat concuerror_report.txt; exit 1; }

.PHONY: concuerror_test
concuerror_test: $(CONCUERROR)
	rebar3 as concuerror eunit -m concuerror_tests
	$(call concuerror,cvar_read_test)
	$(call concuerror,cvar_unset_test)
	$(call concuerror,cvar_double_wait_test)
	$(call concuerror,cvar_waiter_killed_test)
	$(call concuerror,cvar_wait_multiple_test)
	$(call concuerror,cvar_wait_multiple_timeout_test)
	$(call concuerror,wait_for_shards_inf_test)
	# $(call concuerror,wait_for_shards_crash_test)
	$(call concuerror,notify_different_tags_test)
	$(call concuerror,get_core_node_test)
	$(call concuerror,dirty_bootstrap_test)
	$(call concuerror,wait_for_shards_timeout_test)


$(CONCUERROR):
	mkdir -p _build/
	cd _build && git clone https://github.com/parapluu/Concuerror.git
	$(MAKE) -C _build/Concuerror/

##########################################################################################
# Docs
##########################################################################################
DOC_DIR=doc
DOC_SRC_DIR=$(DOC_DIR)/src

UMLS=$(wildcard $(DOC_SRC_DIR)/*.uml)
PICS=$(UMLS:$(DOC_SRC_DIR)/%.uml=$(DOC_DIR)/%.png)

.PHONY: doc
doc: $(PICS)

$(DOC_DIR)/%.png: $(DOC_SRC_DIR)/%.uml
	cat $< | plantuml -pipe > $@
