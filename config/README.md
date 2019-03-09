# iReceptor AIRR Mapping Configuration File

One of the challenges of maintaining a platform that federates data across multiple repositories
is the mapping of terms between those repositories. In order for data to be interoperable (the I 
in the FAIR data principles of being Findable, Accessible, Interoperable, and Reusable) between repositories it
is necessary to have a well, defined consistent representation of the data. The iReceptor Platform
relies on the AIRR
Minimal Standards (MiAIRR) specification and the accompanying API and file formats to enable this
interoperability and facilitate data sharing. In addition, each of the components of the platform
might independently choose to represent data in a different way. The best way to illustrate this it to
consider a concrete example. The MiAIRR minimal standard defines one of the fields that AIRR-seq data
should store is the "V segment gene and allele" of a specific rearrangement that has been acquired through
sequencing a specific repertoire from a specific subject. This field is defined in the MiAIRR standard as
follows:

- MiAIRR field designation = V gene
- Data type	= string
- Content format = Free text
- MiAIRR content definition = V segment gene and allele
- Field value example = IGHV1-34\*01
- AIRR Formats WG field name = v_call

If you consider how this piece of information is represented in each of the components of the iReceptor 
Platform, you need to consider how annotation tools, data repositories, web services, web APIs, and
web applications represent this information. In addition, it is important to be able to transform the data
element (v_call) from one representation to the other. Rather than have different code bases that manage the
different versions, we utilize a mapping file that handles these mappings across the entire iReceptor Platform.

Components where mappings are required include:

- Annotation tools: There are a range of annotation tools, each of which produce output in different file formats. Each tool computes a representation of v_call and stores it in a custom output file.
	- igblast uses the AIRR TSV format, and has a column named "v_call"
	- IMGT HighV-QUEST uses a custom file format that consists of multiple files, and v_call is captured in the file "1_Summary.txt" in the column "V-GENE and allele" 
	- MiXCR uses a custom file format with v_call captured in a column named "bestVHit"
- Repositories: Although it makes sense for a new repository to use the MiAIRR field names directly, for repositories that pre-existed the MiAIRR standard this will not be the case. In addition, as repositories change, field names may also change. In general, it is useful to be able to map a repository field name (and indeed, possible a repository table and field name) to a MiAIRR equivalent field name.
	- iReceptor Public Archive v0.1: This is the early version of the iReceptor public archive, which pre-existed the MiAIRR standard. In these repositories the v_call field was internally represented in a database field called "v_gene".
	- iReceptor Public Archive v1.0: This version of the iReceptor Repository was created after the establishment of the MiAIRR standard, but its field names are based on MiAIRR v1.0.0 (Oct 2017)
	- iReceptor Public Archive Turnkey v1.0: This version of the iReceptor Repository is based on MiAIRR v1.2 (Aug 2018). Although changes are relatively small between the MiAIRR versions, some refinements were made to the MiAIRR fields.
- Data Loading: Data loaders map data from annotation tools into data that is stored in a AIRR-seq Repository. Thus a data loader needs to map file formats (igblast, IMGT HighV-QUEST, and MiXCR) into repositories representations (IPA v0.1, IPA v1.0, IPA Turnkey v1.0)
- Web APIs: AIRR Web APIs are the mechanism by which clients of the AIRR Data Commons perform queries against AIRR-seq repositories. The AIRR API, like the AIRR standards, implement different versions that change over time. Specific versions of the AIRR API implement specific versions of the MiAIRR standard. In addition, capabilities of the AIRR API change over time, meaning that different MiAIRR fields may or may not be supported. 
- Web Services: Web services in the iReceptor Platform translate Web API requests (in a specific version) into queries against a AIRR-seq Repository. Thus they must be able to translate requests from a specific AIRR API version into a query against a specific version of a AIRR-seq Repository.
- Web Applications: The iReceptor Scientific Gateway is a web application that allows users to explore data across the entire AIRR Data Commons. It performs queries on repositories in the AIRR Data Commons on behalf of the user and federates those results. Thus it needs to map MiAIRR field names into visual representations in the user interface as well as map field names to Web API queries for a specific API version.


## AIRR Mapping File Format

The AIRR Mapping file is a tab delimited text file, with each column representing a platform component and version combination and each row representing a "field". The primary fields (rows) are equivalent to the MiAIRR field names, but the iReceptor Platform also utilizes some custom fields. These custom fields may be used in one or more of the platform components, including the iReceptor Gateway, an iReceptor Repository, or the AIRR API. 

There are a range of component columns (e.g. one entry in each column for v_call), including:

- iReceptor specific columns
	- ir_id: This is the column that contains the internal unique row for the field used by the iReceptor Platform.
	- ir_v2: This maps an ir_id into a specific iReceptor Platform version implementation.
- iReceptor Gateway columns
	- ir_short: A short, textual description for the field that can be used by UI components.
	- ir_full: A longer description of the field, typically the same as the "MiAIRR content definition"
- Annotation tool columns
	- vquest_file: The IMGT HighV-QUEST output file in which the the field is found
	- vquest: The IMGT HighV-QUEST field, in the file specified above, in which the the field is found
	- igblast: The name of the field in the igblast output file in which the field is found.
	- mixcr: The name of the field in the mixcr output file in which the field is found.
- Data curation columns:
	- ir_curator_required: Denotes whether the field is required by the data loading and curation process.
	- ir_curator: The name of the field in the data loading meta-data representation used by the data loader.
- Repository mapping:
	- ir_turnkey: The mapping of fields used in the iReceptor Turnkey repository.
	- ir_ipa: The mapping of fields used in the iReceptor Public Archive (v1.0) repository.
	- ir_v1: The mapping of fields used in the iReceptor Public Archive (v0.1) repository.
	- ir_sql: The mapping of fields used in the original iReceptor Public Archive SQL repository.
- AIRR Representations: These are essentially the same set of fields from MiAIRR described above.
	- ir_class: The MiAIRR "class" for the field, either repertoire or rearrangement.
	- ir_subclass: The MiAIRR "subset" for the data field (e.g. study, subject, sample).
	- airr: The MiAIRR "computable" field name from the AIRR Data Representation Standard.
	- airr_type: The type of the field according to the MiAIRR Standard (e.g. String, Boolean, Unsigned integer)
	- airr_full: A description of the MiAIRR field.
	- airr_desc: A longer description of the field and its uses.
	- airr_exp: An example of a valid value for the field.

Note that these fields are extensible and can be used in various ways by iReceptor Platform components. For example, it would be possible for a repository to create their own AIRR Mapping column and use this same configuration file to use the iReceptor Data Loading module to load data into their repository.
