PYTHON ?= python3
CONFIG ?= configs/config.yaml
PYTEST_ARGS ?=

.PHONY: install test mkv-clean mkv-scan rename file-scan file-rename config-loader

install:
	$(PYTHON) -m pip install --upgrade pip setuptools wheel
	$(PYTHON) -m pip install -e .[dev]

test:
	$(PYTHON) -m pytest $(PYTEST_ARGS)

mkv-clean:
	./apps/vid-mkv-clean $(CONFIG)

mkv-scan:
	./apps/scan-tracks $(CONFIG)

rename:
	./apps/vid-rename $(CONFIG)

file-scan:
	./apps/file-scan $(CONFIG)

file-rename:
	./apps/file-rename $(CONFIG)

config-loader:
	$(if $(TASK),,$(error TASK is required, e.g. make config-loader TASK=vid_mkv_clean))
	$(PYTHON) -m common.shared.loader $(TASK) $(CONFIG)
