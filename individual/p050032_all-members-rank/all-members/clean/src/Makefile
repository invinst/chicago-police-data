all: ../output/all-members.csv.gz
	
../output/all-members.csv.gz: \
	../output/all-members.csv
	gzip -k $< ../output/all-members.csv.gz

../output/all-members.csv:
	clean.py \
	../input/all-members.csv \
	../output/all-members.csv \
	python $<
