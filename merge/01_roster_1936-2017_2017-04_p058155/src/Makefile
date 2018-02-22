# -*- coding: utf-8 -*-

# Authors:	Roman Rivera (Invisible Institute)
# merge/01_roster_1936-2017_2017-04_p058155/src/Makefile

.PHONY: all clean
# Additional files created: merge.log, merge.yaml,
# roster_1936-2017_2017-04.csv.gz

all: \
		output/officer-reference.csv.gz

output/officer-reference.csv.gz: \
		src/merge.py \
		input/roster_1936-2017_2017-04.csv.gz \
		input/roster_1936-2017_2017-04_profiles.csv.gz
	python $<


clean:
	rm output/*


# done
