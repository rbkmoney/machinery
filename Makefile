REBAR := $(shell which rebar3 2>/dev/null || which ./rebar3)

UTILS_PATH := build_utils
TEMPLATES_PATH := .

SUBMODULES = $(UTILS_PATH)
SUBTARGETS = $(patsubst %,%/.git,$(SUBMODULES))

SERVICE_NAME := machinery
BASE_IMAGE_NAME := service-erlang
BASE_IMAGE_TAG := 54a794b4875ad79f90dba0a7708190b3b37d584f
BUILD_IMAGE_NAME := build-erlang
BUILD_IMAGE_TAG := 19ff48ccbe09b00b79303fc6e5c63a3a9f8fd859

CALL_ANYWHERE := all submodules compile xref lint dialyze clean distclean check_format format

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

check_format:
	$(REBAR) as test fmt -c

format:
	$(REBAR) fmt -w

dialyze: submodules
	$(REBAR) dialyzer

clean:
	$(REBAR) clean

distclean:
	$(REBAR) clean -a
	rm -rf _build

test: submodules
	$(REBAR) ct
