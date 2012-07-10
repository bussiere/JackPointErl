LIBDIR=`erl -eval 'io:format("~s~n", [code:lib_dir()])' -s init stop -noshell`
VERSION=0.2
PKGNAME=erlmc

.PHONY: deps

all: deps compile

compile:
	@./rebar compile

debug:
	@./rebar debug_info=1 compile

deps:
	@./rebar get-deps

clean:
	@./rebar clean

test:
	@./rebar compile skip_deps=true eunit

install:
	mkdir -p $(prefix)/$(LIBDIR)/$(PKGNAME)-$(VERSION)/{ebin,include}
	for i in ebin/*.beam ebin/*.app include/*.hrl; do install $$i $(prefix)/$(LIBDIR)/$(PKGNAME)-$(VERSION)/$$i ; done
