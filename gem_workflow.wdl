task run_tests {

	File genofile
	Float maf
	File? samplefile
	File phenofile
	String sample_id_header
	String outcome
	Boolean binary_outcome
	String exposure_names
	String? int_covar_names
	String? covar_names
	String delimiter
	String missing
	Boolean robust
	Float tol
	Int threads
	Int stream_snps
	Int memory
	Int cpu
	Int disk
	Int monitoring_freq

	String binary_outcome01 = if binary_outcome then "1" else "0"
	String robust01 = if robust then "1" else "0"

	command {
		dstat -c -d -m --nocolor ${monitoring_freq} > system_resource_usage.log &
		atop -x -P PRM ${monitoring_freq} | grep '(GEM)' > process_resource_usage.log &

		/GEM/GEM \
			--bgen ${genofile} \
			--maf ${maf} \
			${"--sample " + samplefile} \
			--pheno-file ${phenofile} \
			--sampleid-name ${sample_id_header} \
			--pheno-name ${outcome} \
			--pheno-type ${binary_outcome01} \
			--exposure-names ${exposure_names} \
			${"--int-covar-names " + int_covar_names} \
			${"--covar-names " + covar_names} \
			--delim ${delimiter} \
			--missing-value ${missing} \
			--robust ${robust01} \
			--tol ${tol} \
			--threads ${threads} \
			--stream-snps ${stream_snps} \
			--out gem_res
	}

	runtime {
		docker: "quay.io/large-scale-gxe-methods/gem-workflow:dev"
		memory: "${memory} GB"
		cpu: "${cpu}"
		disks: "local-disk ${disk} HDD"
		gpu: false
		dx_timeout: "7D0H00M"
	}

	output {
		File out = "gem_res"
		File system_resource_usage = "system_resource_usage.log"
		File process_resource_usage = "process_resource_usage.log"
	}
}

task cat_results {

	Array[File] results_array

	command {
		head -1 ${results_array[0]} > all_results.txt && \
			for res in ${sep=" " results_array}; do tail -n +2 $res >> all_results.txt; done
	}
	
	runtime {
		docker: "ubuntu:latest"
		disks: "local-disk 10 HDD"
	}
	output {
		File all_results = "all_results.txt"
	}
}


workflow run_GEM {

	Array[File] genofiles
	Float? maf = 0.005
	File? samplefile
	File phenofile
	String? sample_id_header = "sampleID"
	String outcome
	Boolean binary_outcome
	String exposure_names
	String? int_covar_names
	String? covar_names
	String? delimiter = ","
	String? missing = "NA"
	Boolean robust
	Int? stream_snps = 1
	Float? tol = 0.000001
	Int? memory = 10
	Int? cpu = 4
	Int? disk = 50
	Int? threads = 2
	Int? monitoring_freq = 1

	scatter (i in range(length(genofiles))) {
		call run_tests {
			input:
				genofile = genofiles[i],
				maf = maf,
				samplefile = samplefile,
				phenofile = phenofile,
				sample_id_header = sample_id_header,
				outcome = outcome,
				binary_outcome = binary_outcome,
				exposure_names = exposure_names,
				int_covar_names = int_covar_names,
				covar_names = covar_names,
				delimiter = delimiter,
				missing = missing,
				robust = robust,
				stream_snps = stream_snps,
				tol = tol,
				memory = memory,
				cpu = cpu,
				disk = disk,
				threads = threads,
				monitoring_freq = monitoring_freq
		}
	}

	call cat_results {
		input:
			results_array = run_tests.out
	}

	output {
		File results = cat_results.all_results
		Array[File] system_resource_usage = run_tests.system_resource_usage
		Array[File] process_resource_usage = run_tests.process_resource_usage
	}

	parameter_meta {
		genofiles: "Array of genotype filepaths in .bgen format."
		maf: "Minor allele frequency threshold for pre-filtering variants as a fraction (default is 0.005)."
		samplefile: "Optional .sample file accompanying the .bgen file. Required for proper function if .bgen does not store sample identifiers."
		phenofile: "Phenotype filepath."	
		sample_id_header: "Optional column header name of sample ID in phenotype file."
		outcome: "Column header name of phenotype data in phenotype file."
                binary_outcome: "Boolean: is the outcome binary? Otherwise, quantitative is assumed."
		exposure_names: "Column header name(s) of the exposures for genotype interaction testing (space-delimited)."
		int_covar_names: "Column header name(s) of any covariates for which genotype interactions should be included for adjustment in regression (space-delimited). These terms will not be included in any multi-exposure interaction tests. This set should not overlap with exposures or covar_names."
		covar_names: "Column header name(s) of any covariates for which only main effects should be included selected covariates in the pheno data file (space-delimited). This set should not overlap with exposures or int_covar_names."
		delimiter: "Delimiter used in the phenotype file."
		missing: "Missing value key of phenotype file."
                robust: "Boolean: should robust (a.k.a. sandwich/Huber-White) standard errors be used?"
		stream_snps: "SNP numbers for each GWAS analysis."
		tol: "Convergence tolerance for logistic regression."
		memory: "Requested memory (in GB)."
		cpu: "Minimum number of requested cores."
		disk: "Requested disk space (in GB)."
		threads: "Number of threads GEM should use for parallelization over variants."
		monitoring_freq: "Delay between each output for process monitoring (in seconds). Default is 1 second."
	}

        meta {
                author: "Kenny Westerman"
                email: "kewesterman@mgh.harvard.edu"
                description: "Run interaction tests using GEM and return a table of summary statistics for K-DF interaction and (K+1)-DF joint tests."
        }
}

