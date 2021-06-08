BUILD_DIR := $(CURDIR)/_build

REBAR := rebar3

CT_NODE_NAME = ct@127.0.0.1

.PHONY: all
all: compile

compile:
	$(REBAR) do compile, dialyzer, xref

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

.PHONY: ct
ct: compile
	$(REBAR) do eunit, ct -v --readable=false --name $(CT_NODE_NAME)

cover:
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
	$(verbose) $(CUTTLEFISH_SCRIPT) -l info -e etc/ -c etc/ekka.conf.example -i priv/ekka.schema -d data/

##########################################################################################
# Concuerror
##########################################################################################

CONCUERROR := $(BUILD_DIR)/Concuerror/bin/concuerror
CONCUERROR_RUN := $(CONCUERROR) \
	--treat_as_normal shutdown --treat_as_normal normal \
	-x code -x code_server -x error_handler \
	-pa $(BUILD_DIR)/concuerror+test/lib/snabbkaffe/ebin \
	-pa $(BUILD_DIR)/concuerror+test/lib/ekka/ebin

concuerror = $(CONCUERROR_RUN) -f $(BUILD_DIR)/concuerror+test/lib/ekka/test/concuerror_tests.beam -t $(1) || \
	{ cat concuerror_report.txt; exit 1; }

.PHONY: concuerror_test
concuerror_test: $(CONCUERROR)
	rebar3 as concuerror eunit -m concuerror_tests
	$(call concuerror,wait_for_shards_inf_test)
	$(call concuerror,wait_for_shards_timeout_test)
	$(call concuerror,wait_for_shards_crash_test)
	$(call concuerror,notify_different_tags_test)
	$(call concuerror,get_core_node_test)

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
