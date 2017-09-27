# -*- coding: utf-8 -*-

# Authors:	Roman Rivera
# merge/final-profiles/src/Makefile

.PHONY: all clean
# Additional files created: final-profiles.log, final-profiles.yaml

all: \
		output/final-profiles.csv.gz

output/final-profiles.csv.gz: \
		src/final-profiles.py \
                input/officer-reference.csv.gz 
	python $<


clean:
	rm output/*


# done
