REBAR := $(or $(shell which rebar3), $(error "`rebar3' executable missing"))

UTILS_PATH := build_utils
TEMPLATES_PATH := .

SUBMODULES = $(UTILS_PATH)
SUBTARGETS = $(patsubst %,%/.git,$(SUBMODULES))

SERVICE_NAME := mg-api
BUILD_IMAGE_TAG := eee42f2ca018c313190bc350fe47d4dea70b6d27

CALL_ANYWHERE := all submodules compile xref lint dialyze release clean distclean

CALL_W_CONTAINER := $(CALL_ANYWHERE) test

all: compile

-include $(UTILS_PATH)/make_lib/utils_container.mk

.PHONY: $(CALL_W_CONTAINER)

$(SUBTARGETS): %/.git: %
	git submodule update --init $<
	touch $@

submodules: $(SUBTARGETS)

compile: submodules
	$(REBAR) compile

xref: submodules
	$(REBAR) xref

lint:
	elvis rock

dialyze: submodules
	$(REBAR) dialyzer

clean:
	$(REBAR) clean

distclean:
	$(REBAR) clean -a
	rm -rf _build

test: submodules
	$(REBAR) ct
