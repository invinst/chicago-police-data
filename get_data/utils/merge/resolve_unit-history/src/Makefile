# -*- coding: utf-8 -*-

# Authors:	Roman Rivera (Invisible Institute)
# merge/resolve_unit-history/src/Makefile

.PHONY: all clean
# Additional files created: resolve_unit-history.log, resolve_unit-history.yaml
all: \
		output/unit-history.csv.gz

output/unit-history.csv.gz: \
		src/resolve_unit-history.py \
    input/officer-reference.csv.gz \
		input/unit-history__2016-03.csv.gz \
		input/unit-history__2016-12.csv.gz
	python $<


clean:
	rm output/*


# done
