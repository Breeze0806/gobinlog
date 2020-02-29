export GO15VENDOREXPERIMENT=1

PRO_PATH=github.com/Breeze0806/gobinlog
PKGS = ${PRO_PATH} ${PRO_PATH}/replication
# Many Go tools take file globs or directories as arguments instead of packages.
PKG_FILES ?=*.go replication
COVERALLS_TOKEN=WrkOJBvlULyqJtq7IeT5c8FcST2mkEy0q
# The linting tools evolve with each Go version, so run them only on the latest
# stable release.
GO_VERSION := $(shell go version | cut -d " " -f 3)
GO_MINOR_VERSION := $(word 2,$(subst ., ,$(GO_VERSION)))
LINTABLE_MINOR_VERSIONS := 12
ifneq ($(filter $(LINTABLE_MINOR_VERSIONS),$(GO_MINOR_VERSION)),)
SHOULD_LINT := true
endif

.PHONY: all
all: lint test examples

.PHONY: dependencies
dependencies:
	@echo "Installing test dependencies..."
	go get github.com/mattn/goveralls
	go get github.com/Breeze0806/mysql
	go get github.com/go-sql-driver/mysql
ifdef SHOULD_LINT
	@echo "Installing golint..."
	go get -u golang.org/x/lint/golint
else
	@echo "Not installing golint, since we don't expect to lint on" $(GO_VERSION)
endif

.PHONY: lint
lint:
ifdef SHOULD_LINT
	@rm -rf lint.log
	@echo "Checking formatting..."
	@gofmt -d -s $(PKG_FILES) 2>&1 | tee lint.log
	@echo "Installing test dependencies for vet..."
	@go test -i $(PKGS)
	@echo "Checking vet..."
	@go vet $(VET_RULES) $(PKGS) 2>&1 | tee -a lint.log
	@echo "Checking lint..."
	@$(foreach dir,$(PKGS),golint $(dir) 2>&1 | tee -a lint.log;)
#	@echo "Checking for unresolved FIXMEs..."
#	@git grep -i fixme | grep -v -e vendor -e Makefile | tee -a lint.log
#	@echo "Checking for license headers..."
#	@./check_license.sh | tee -a lint.log
	@[ ! -s lint.log ]
else
	@echo "Skipping linters on" $(GO_VERSION)
endif

.PHONY: test
test:
	@go test -race ${PKGS}

.PHONY: cover
cover:
	./cover.sh $(PKGS)

.PHONY: cmd
examples:
	@cd cmd/binlogDump && go build

.PHONY: binlogDumpStart
binlogDumpStart:
	@examples/binlogDump/binlogDump -c examples/binlogDump/config/binlogDump.json

.PHONY: doc
doc:
	@godoc -http=:6080