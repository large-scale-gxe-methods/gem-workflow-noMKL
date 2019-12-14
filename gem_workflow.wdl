task rearrange_covars {

	String covar_headers
	String exposures

	command {
		/rearrange_covars.sh "${covar_headers}" "${exposures}"
	}

	runtime {
		docker: "quay.io/large-scale-gxe-methods/gem-workflow"
	}	

	output {
		Array[String] rcout = read_lines(stdout())
		String new_covars = rcout[0]
		String int_covar_num = rcout[1]
	}
}


task run_tests {

	File genofile
	File? samplefile
	File phenofile
	String sample_id_header
	String outcome
	Boolean binary_outcome
	String covar_headers
	String int_covar_num
	String? delimiter = ","
	String? missing = "NaN"
	Boolean robust
	String? stream_snps = 20
	String? tol = "0.000001"
	String out_name
	Int? memory = 10
	Int? cpu = 4
	Int? disk = 20

	String pheno = if binary_outcome then "1" else "0"
	String robust01 = if robust then "1" else "0"

	command {
		

		echo -e "SAMPLE_ID_HEADER\n${sample_id_header}\n"\
			"PHENOTYPE\n${pheno}\n"\
			"PHENO_HEADER\n${outcome}\n"\
			"COVARIATES_HEADERS\n${covar_headers}\n"\
			"MISSING\n${missing}\n"\
			"ROBUST\n${robust01}\n"\
			"STREAM_SNPS\n${stream_snps}\n"\
			"NUM_OF_INTER_COVARIATE\n${int_covar_num}\n"\
			"LOGISTIC_CONVERG_TOL\n${tol}\n"\
			"DELIMINATOR\n${delimiter}\n"\
			"GENO_FILE_PATH\n${genofile}\n"\
			"PHENO_FILE_PATH\n${phenofile}\n"\
			"SAMPLE_FILE_PATH\n${samplefile}\n"\
			"OUTPUT_PATH\n${out_name}_res"\
			> GEM_Input.param

		/GEM/GEM GEM_Input.param
	}

	runtime {
		docker: "quay.io/large-scale-gxe-methods/gem-workflow"
		memory: "${memory} GB"
		cpu: "${cpu}"
		disks: "local-disk ${disk} HDD"
	}

	output {
		File out = "${out_name}_res"
		File param_file = "GEM_Input.param"
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
	File? samplefile
	File phenofile
	String? sample_id_header
	String outcome
	Boolean binary_outcome
	String covar_headers
	String exposures
	String? delimiter
	String? missing
	Boolean robust
	String? stream_snps
	String? tol
	Array[String] out_names
	Int? memory
	Int? cpu
	Int? disk

	call rearrange_covars {
		input:
			covar_headers = covar_headers,
			exposures = exposures
	}

	scatter (i in range(length(genofiles))) {
		call run_tests {
			input:
				genofile = genofiles[i],
				samplefile = samplefile,
				phenofile = phenofile,
				sample_id_header = sample_id_header,
				outcome = outcome,
				binary_outcome = binary_outcome,
				covar_headers = rearrange_covars.new_covars,
				int_covar_num = rearrange_covars.int_covar_num,
				delimiter = delimiter,
				missing = missing,
				robust = robust,
				stream_snps = stream_snps,
				tol = tol,
				out_name = out_names[i],
				memory = memory,
				cpu = cpu,
				disk = disk
		}
	}

	call cat_results {
		input:
			results_array = run_tests.out
	}

	output {
		File results = cat_results.all_results
	}

	parameter_meta {
		genofiles: "Array of genotype filepaths in .bgen format."
		samplefile: "Optional .sample file accompanying the .bgen file. Required for proper function if .bgen does not store sample identifiers."
		phenofile: "Phenotype filepath."	
		sample_id_header: "Column header name of sample ID in phenotype file."
		outcome: "Column header name of phenotype data in phenotype file."
                binary_outcome: "Boolean: is the outcome binary? Otherwise, quantitative is assumed."
		covar_headers: "Column header names of the selected covariates in the pheno data file (space-delimited)."
		exposures: "Column header name(s) of the covariates to use as exposures for genotype interaction testing (space-delimited). All exposures must also be provided as covariates."
		delimiter: "Delimiter used in the phenotype file."
		missing: "Missing value key of phenotype file."
                robust: "Boolean: should robust (a.k.a. sandwich/Huber-White) standard errors be used?"
		stream_snps: "SNP numbers for each GWAS analysis."
		tol: "Convergence tolerance for logistic regression."
		out_names: "Array of names to distinguish output files (e.g. chromosome numbers)."
		memory: "Requested memory (in GB)."
		cpu: "Minimum number of requested cores."
		disk: "Requested disk space (in GB)."
	}

        meta {
                author: "Kenny Westerman"
                email: "kewesterman@mgh.harvard.edu"
                description: "Run interaction tests using GEM and return a table of summary statistics for 1-DF and 2-DF tests."
        }
}

