#!/bin/bash

echo "Type base path of the bam files, do not add / in the end"
read input

echo "how many threads for bam processing?"
read threads

i=0

for f in "$input"/*
do
	base=$(basename $f)
	if [[ $base == *.bam ]]
	then
		echo $base
		read id _ <<< $(IFS="."; echo $base)
		echo $id
		samtools view $f | awk -F '\t' 'function abs(x){return ((x<0.0)? -x:x)} {if(abs($9)>1000) print "'$id'\t"$0}' > "$input"/output/"$base".txt &
		i=$((i+1))
		if (( $i >= $threads))
		then
			wait
			i=0
		fi
	fi
done

wait
#samtools view ./$input/HG00137.chrom20.ILLUMINA.bwa.GBR.low_coverage.20120522.bam | awk -F '\t' 'function abs(x){return ((x<0.0)? -x:x)} {if(abs($9)>1000) print "HG00137\t"$0}' > output.txt
