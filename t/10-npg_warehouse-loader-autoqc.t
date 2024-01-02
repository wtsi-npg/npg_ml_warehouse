use strict;
use warnings;
use Test::More tests => 11;
use Test::Exception;
use Moose::Meta::Class;

use npg_testing::db;

use npg_tracking::glossary::composition;
use npg_tracking::glossary::composition::component::illumina;
use npg_qc::autoqc::qc_store;
use npg_qc::autoqc::results::qX_yield;

use_ok('npg_warehouse::loader::autoqc');

my $compos_pkg = 'npg_tracking::glossary::composition';
my $compon_pkg = 'npg_tracking::glossary::composition::component::illumina';

my $store = npg_qc::autoqc::qc_store->new(use_db => 0);
my $util = Moose::Meta::Class->create_anon_class(
    roles => [qw/npg_testing::db/])->new_object({});
my $schema_npg  = $util->create_test_db(
  q[npg_tracking::Schema], q[t/data/fixtures/npg]);
my $folder_glob = q[t/data/runfolders/];
my $user_id = 7;
my $plex_key = 'plex';

subtest 'object creation and simple tests' => sub {
  plan tests => 3;

  my $autoqc;
  lives_ok {$autoqc = npg_warehouse::loader::autoqc->new( 
                    autoqc_store => $store)}
    'autoqc retriever object instantiated by passing autoqc ' .
    'store object to the constructor';
  isa_ok ($autoqc, 'npg_warehouse::loader::autoqc');
  throws_ok {$autoqc->retrieve()}
    qr/Attribute \(id_run\) does not pass the type constraint/,
    'error when id_run is missing';
};

subtest 'retrieve yield' => sub {
  plan tests => 5;

  my $autoqc = npg_warehouse::loader::autoqc->new( 
                    autoqc_store => $store);

  my $result = npg_qc::autoqc::results::qX_yield->new(
    id_run=>22, position=>1, threshold_quality => 30,);
  lives_ok {$autoqc->_qX_yield($result, {})} 'lives with quality 30';
  $result->yield1(200);
  my @data = $autoqc->_qX_yield($result, {});
  is (scalar @data, 1, 'one result');
  is ($data[0]->{q20_yield_kb_forward_read}, 200, 'retrieved q20 for the forward read');
  ok(!exists $data[0]->{q20_yield_kb_reverse_read}, 'reverse slot does not exist');
  $result->yield2(300);
  @data = $autoqc->_qX_yield($result, {});
  is ($data[0]->{q20_yield_kb_reverse_read}, 300, 'reverse slot filled');
};

subtest 'retrieve data for run 4799' => sub {
  plan tests => 19;

  my $id_run = 4799;
  lives_ok {$schema_npg->resultset('Run')->find({id_run => $id_run})->set_tag($user_id, 'staging')}
    'staging tag is set - test prerequisite';
  lives_ok {$schema_npg->resultset('Run')
    ->update_or_create({folder_path_glob => $folder_glob, folder_name => '100330_HS21_4799', id_run => $id_run, })}
    'forder glob reset lives - test prerequisite';

  my $auto;
  lives_ok { $auto = npg_warehouse::loader::autoqc->new( 
             autoqc_store => $store)->retrieve($id_run, $schema_npg)}
    'data for run 4799 retrieved';
  
  my @compositions = map {
    $compos_pkg->new(components =>
      [$compon_pkg->new(id_run => $id_run, position => $_)] )
                         } (1, 4);
  my $d = $compositions[0]->digest();
  is ($auto->{$d}->{q20_yield_kb_forward_read}, 46671, 'qx forward lane 1');
  is ($auto->{$d}->{q20_yield_kb_reverse_read}, 39877, 'qx reverse lane 1');
  is ($auto->{$d}->{ref_match1_name},  q[Homo sapiens 1000Genomes], 'ref_match name1 lane 1');
  is ($auto->{$d}->{ref_match1_percent}, 95.7, 'ref_match count1 lane 1');
  is ($auto->{$d}->{ref_match2_name},  q[Gorilla gorilla gorilla], 'ref_match name2 lane 1');
  is ($auto->{$d}->{ref_match2_percent}, 85.2, 'ref_match count2 lane 1');
  is ($auto->{$d}->{'composition'}->digest, $d, 'composition object saved');
  $d = $compositions[1]->digest();
  is ($auto->{$d}->{ref_match1_name},  q[Homo sapiens 1000Genomes], 'ref_match name1 lane 4');
  is ($auto->{$d}->{ref_match1_percent}, 97.2, 'ref_match count1 lane 4');
  is ($auto->{$d}->{ref_match2_name},  q[Gorilla gorilla gorilla], 'ref_match name2 lane 4');
  is ($auto->{$d}->{ref_match2_percent}, 87.2, 'ref_match count2 lane 4');

  @compositions = map {
    $compos_pkg->new( components=>
      [$compon_pkg->new(id_run => $id_run, position => 3, tag_index => $_)] )
                         } (0, 3);
  $d = $compositions[0]->digest();
  is ($auto->{$d}->{q20_yield_kb_forward_read}, 46671, 'qx reverse tag 0');
  is ($auto->{$d}->{q20_yield_kb_reverse_read}, 39877, 'qx reverse tag 0');
  is ($auto->{$d}->{'composition'}->digest, $d, 'composition object saved');
  $d = $compositions[1]->digest();
  is ($auto->{$d}->{q20_yield_kb_forward_read}, 1455655, 'qx forward tag 3');
  is ($auto->{$d}->{q20_yield_kb_reverse_read}, 1393269, 'qx reverse tag 3');
};

subtest 'retrieve data for run 4333' => sub {
  plan tests => 14;

  my $id_run = 4333;
  lives_ok {$schema_npg->resultset('Run')->find({id_run => $id_run})->set_tag($user_id, 'staging')}
    'staging tag is set - test prerequisite';
  lives_ok {$schema_npg->resultset('Run')
    ->update_or_create({folder_path_glob => $folder_glob, folder_name => '100330_IL21_4333', id_run => $id_run, })}
    'forder glob reset lives - test prerequisite';
  my $auto;
  lives_ok {$auto = npg_warehouse::loader::autoqc->new( 
    autoqc_store => $store)->retrieve($id_run, $schema_npg)}
  'data for run 4333 retrieved';

  my $d = $compos_pkg->new(components =>
    [$compon_pkg->new(id_run => $id_run, position => 4)])->digest;
  is ($auto->{$d}->{insert_size_quartile1}, 172,   'insert size q1');
  is ($auto->{$d}->{insert_size_quartile3}, 207,   'insert size q3');
  is ($auto->{$d}->{insert_size_median},    189,   'insert size median');
  is ($auto->{$d}->{insert_size_num_modes},             1,     'insert size num modes');
  is ($auto->{$d}->{insert_size_normal_fit_confidence}, '0.35',   'insert size norm fit conf');

  my @compositions = map {
    $compos_pkg->new( components=>
      [$compon_pkg->new(id_run => $id_run, position => 1, tag_index => $_)] )
                         } (1, 3, 6);
  $d = $compositions[0]->digest;
  is ($auto->{$d}->{tag_sequence}, 'ATCACG', 'lane 1 tag index 1 tag sequence');
  $d = $compositions[1]->digest;
  ok (exists $auto->{$d}, 'lane 1 tag index 1 present in an autoqc plex hash');
  $d = $compositions[2]->digest;
  is ($auto->{$d}->{tag_decode_percent}, 3.49, 'lane 1 tag index 6 tag decode percent');
  is ($auto->{$d}->{tag_decode_count}, 1111591, 'lane 1 tag index 6 tag decode count');

  $d = $compos_pkg->new(components =>
    [$compon_pkg->new(id_run => $id_run, position => 1)])->digest;
  cmp_ok(sprintf('%.2f', $auto->{$d}->{tags_decode_percent}), q(==), 99.29, 'lane 1 tag decode percent from tag decode stats');
  cmp_ok(sprintf('%.2f', $auto->{$d}->{tags_decode_cv}), q(==), 55.1, 'lane 1 tag coeff of var from tag decode stats');
};

subtest 'retrieve data for run 6624' => sub {
  plan tests => 37;

  my $id_run = 6624;
  lives_ok {$schema_npg->resultset('Run')->find({id_run => $id_run})->set_tag($user_id, 'staging')}
    'staging tag is set - test prerequisite';
  lives_ok {$schema_npg->resultset('Run')
    ->update_or_create({folder_path_glob => $folder_glob, folder_name => '110731_HS17_06624_A_B00T5ACXX', id_run => $id_run, })}
    'forder glob reset lives - test prerequisite';

  my $auto;
  lives_ok {$auto = npg_warehouse::loader::autoqc->new(
    autoqc_store => $store)->retrieve($id_run, $schema_npg);}
    'data for run 6624 retrieved';
  
  my $d = $compos_pkg->new(components =>
    [$compon_pkg->new(id_run => $id_run, position => 3, tag_index => 4)])->digest;
  cmp_ok(sprintf('%.2f', $auto->{$d}->{bam_num_reads}), q(==), 33605036, 'bam number of reads');
  cmp_ok(sprintf('%.2f', $auto->{$d}->{bam_percent_mapped}), q(==), 96.12, 'bam mapped percent');
  cmp_ok(sprintf('%.2f', $auto->{$d}->{bam_percent_duplicate}), q(==), 1.04, 'bam duplicate percent');

  $d = $compos_pkg->new(components =>
    [$compon_pkg->new(id_run => $id_run, position => 3, tag_index => 1)])->digest;
  is($auto->{$d}->{genotype_sample_name_match}, '23/25', 'gt_sample_name_match check');
  is($auto->{$d}->{genotype_sample_name_relaxed_match}, '24/25', 'gt_sample_name_relaxed_match check');
  cmp_ok(sprintf('%.2f', $auto->{$d}->{genotype_mean_depth}), q(==), 50.12, 'genotype_mean_depth check');
  cmp_ok(sprintf('%.2f', $auto->{$d}->{mean_bait_coverage}), q(==), 41.49, 'mean bait coverage');
  cmp_ok(sprintf('%.2f', $auto->{$d}->{on_bait_percent}), q(==), 68.06, 'on bait percent');
  cmp_ok(sprintf('%.2f', $auto->{$d}->{on_or_near_bait_percent}), q(==), 88.92, 'on or near bait percent');
  cmp_ok(sprintf('%.2f', $auto->{$d}->{sub_ctoa_art_predicted_level}), q(==), 0, 'sub C2A predicted level');
  cmp_ok(sprintf('%.2f', $auto->{$d}->{sub_ctoa_oxh}), q(==), 0.67, 'sub C2A oxh');
  cmp_ok(sprintf('%.2f', $auto->{$d}->{sub_oxog_bias}), q(==), 0.52, 'sub C2A oxog bias'); 
  cmp_ok(sprintf('%.2f', $auto->{$d}->{sub_cv_ti}), q(==), 0.04, 'sub C2A cv ti'); 
  cmp_ok(sprintf('%.2f', $auto->{$d}->{sub_sym_gt_ca}), q(==), 0.52, 'sub C2A sym_gt_ca'); 
  cmp_ok(sprintf('%.2f', $auto->{$d}->{sub_sym_ct_ga}), q(==), 0.07, 'sub C2A sym_ct_ga'); 
  cmp_ok(sprintf('%.2f', $auto->{$d}->{sub_sym_ag_tc}), q(==), 0.03, 'sub C2A sym_ag_tc'); 

  $d = $compos_pkg->new(components =>
    [$compon_pkg->new(id_run => $id_run, position => 3, tag_index => 2)])->digest;
  cmp_ok(sprintf('%.2f', $auto->{$d}->{mean_bait_coverage}), q(==), 42.64, 'mean bait coverage');
  cmp_ok(sprintf('%.2f', $auto->{$d}->{on_bait_percent}), q(==), 68.26, 'on bait percent');
  cmp_ok(sprintf('%.2f', $auto->{$d}->{on_or_near_bait_percent}), q(==), 89.09, 'on or near bait percent'); 

  $d = $compos_pkg->new(components =>
    [$compon_pkg->new(id_run => $id_run, position => 1)])->digest;
  cmp_ok(sprintf('%.2f', $auto->{$d}->{tags_decode_percent}), q(==), 98.96, 'lane 1 tag decode percent from tag metrics');
  cmp_ok(sprintf('%.2f', $auto->{$d}->{tag_hops_percent}), q(==), 1.23, 'lane 1 percent tag hops from tag metrics');
  cmp_ok(sprintf('%.2f', $auto->{$d}->{tag_hops_power}), q(==), 0.85, 'lane 1 tag hops power from tag metrics');
  cmp_ok(sprintf('%.2f', $auto->{$d}->{tags_decode_cv}), q(==), 11.78, 'lane 1 tag decode coeff of var from tag metrics');

  $d = $compos_pkg->new(components =>
    [$compon_pkg->new(id_run => $id_run, position => 2)])->digest;
  cmp_ok(sprintf('%.2f', $auto->{$d}->{tags_decode_percent}), q(==), 98.96,
    'lane 2 tag decode percent from tag metrics in presence of tag decode stats');
  cmp_ok(sprintf('%.2f', $auto->{$d}->{tags_decode_cv}), q(==), 11.69,
    'lane 2 tag matrics coeff of var from tag metrics in presence of tag decode stats');
    
  $d = $compos_pkg->new(components =>
    [$compon_pkg->new(id_run => $id_run, position => 3)])->digest;
  cmp_ok(sprintf('%.2f', $auto->{$d}->{tags_decode_percent}), q(==), 99.05,
    'lane 3 tag decode percent from tag decode stats in absence of tag metrics');

  $d = $compos_pkg->new(components =>
    [$compon_pkg->new(id_run => $id_run, position => 4)])->digest;
  cmp_ok(sprintf('%.2f', $auto->{$d}->{tags_decode_percent}), q(==), 98.96, 'lane 4 tag decode percent from tag metrics');

  $d = $compos_pkg->new(components =>
    [$compon_pkg->new(id_run => $id_run, position => 5)])->digest;
  cmp_ok(sprintf('%.2f', $auto->{$d}->{tags_decode_percent}), q(==), 98.98, 'lane 5 tag decode percent from tag metrics');

  $d = $compos_pkg->new(components =>
    [$compon_pkg->new(id_run => $id_run, position => 1, tag_index => 1)])->digest;
  is ($auto->{$d}->{tag_sequence}, 'ATCACGTT', 'lane 1 tag index 1 tag sequence');

  $d = $compos_pkg->new(components =>
    [$compon_pkg->new(id_run => $id_run, position => 1, tag_index => 0)])->digest;
  ok (!exists $auto->{$d}->{tag_sequence}, 'index zero tag sequence is not defined');
  is ($auto->{$d}->{tag_decode_count}, 1831358, 'lane 1 tag index 0 count');

  $d = $compos_pkg->new(components =>
    [$compon_pkg->new(id_run => $id_run, position => 2, tag_index => 168)])->digest;
  is ($auto->{$d}->{tag_sequence}, 'ACAACGCA', 'lane 2 tag index 168 tag sequence');
  is ($auto->{$d}->{tag_decode_count}, 1277701, 'lane 2 tag index 168 count');
  cmp_ok (sprintf('%.2f', $auto->{$d}->{tag_decode_percent}), q(==), 0.73, , 'lane 2 tag index 168 percent');
};

subtest 'retrieve data for run 6642' => sub {
  plan tests => 24;

  my $id_run = 6642;
  lives_ok {$schema_npg->resultset('Run')->update_or_create({folder_path_glob => $folder_glob, id_run => $id_run, })}
    'forder glob reset lives - test prerequisite';
  lives_ok {$schema_npg->resultset('Run')->find({id_run => $id_run})->set_tag($user_id, 'staging')}
    'staging tag is set - test prerequisite';

  my $auto = npg_warehouse::loader::autoqc->new(autoqc_store => $store)->retrieve($id_run, $schema_npg);

  my $d = $compos_pkg->new(components =>
    [$compon_pkg->new(id_run => $id_run, position => 4)])->digest;
  cmp_ok(sprintf('%.5f',$auto->{$d}->{verify_bam_id_score}), q(==), 0.00166, 'verify_bam_id_score');
  cmp_ok(sprintf('%.2f',$auto->{$d}->{verify_bam_id_average_depth}), q(==), 9.42, 'verify_bam_id_average_depth');
  cmp_ok($auto->{$d}->{verify_bam_id_snp_count}, q(==), 1531960, 'verify_bam_id_snp_count');

  $d = $compos_pkg->new(components =>
    [$compon_pkg->new(id_run => $id_run, position => 3)])->digest;
  cmp_ok(sprintf('%.2f',$auto->{$d}->{bam_percent_mapped}), q(==), 98.19, 'bam mapped percent');
  cmp_ok(sprintf('%.2f',$auto->{$d}->{bam_percent_duplicate}), q(==), 24.63, 'bam duplicate percent');
  
  $d = $compos_pkg->new(components =>
    [$compon_pkg->new(id_run => $id_run, position => 2, tag_index => 4)])->digest;
  cmp_ok(sprintf('%.2f',$auto->{$d}->{bam_human_percent_mapped}), q(==), 55.3, 'bam human mapped percent');
  cmp_ok(sprintf('%.2f',$auto->{$d}->{bam_human_percent_duplicate}), q(==), 68.09, 'bam human duplicate percent');
  cmp_ok(sprintf('%.2f',$auto->{$d}->{bam_num_reads}), q(==), 138756624, 'bam (nonhuman) number of reads');
  cmp_ok(sprintf('%.2f',$auto->{$d}->{bam_percent_mapped}), q(==), 96.3, 'bam (nonhuman) mapped percent');
  cmp_ok(sprintf('%.2f',$auto->{$d}->{bam_percent_duplicate}), q(==), 6.34, 'bam (nonhuman) duplicate percent');

  $d = $compos_pkg->new(components =>
    [$compon_pkg->new(id_run => $id_run, position => 2, tag_index => 5)])->digest;
  cmp_ok(sprintf('%.2f',$auto->{$d}->{bam_human_percent_mapped}), q(==), 55.3, 'bam xahuman mapped percent as human');
  cmp_ok(sprintf('%.2f',$auto->{$d}->{bam_human_percent_duplicate}), q(==), 68.09, 'bam duplicate percent as human');
  cmp_ok(sprintf('%.2f',$auto->{$d}->{bam_percent_mapped}), q(==), 96.3, 'bam xahuman mapped percent as human');
  cmp_ok(sprintf('%.2f',$auto->{$d}->{bam_percent_duplicate}), q(==), 6.34, 'bam duplicate percent as human');

  $d = $compos_pkg->new(components =>
    [$compon_pkg->new(id_run => $id_run, position => 2, tag_index => 6)])->digest;
  cmp_ok(sprintf('%.2f',$auto->{$d}->{bam_human_percent_mapped}), q(==), 55.3, 'bam yhuman mapped percent as human');
  cmp_ok(sprintf('%.2f',$auto->{$d}->{bam_human_percent_duplicate}), q(==), 68.09, 'bam yhuman duplicate percent as human');
  is ($auto->{$d}->{bam_percent_mapped}, undef, 'data not set');
  is ($auto->{$d}->{bam_percent_duplicate}, undef, 'data not set');
  cmp_ok(sprintf('%.2f', $auto->{$d}->{sub_ctoa_art_predicted_level}), q(==), 0, 'sub C2A predicted level');
  cmp_ok(sprintf('%.2f', $auto->{$d}->{sub_ctoa_oxh}), q(==), 0.56, 'sub C2A oxh');
  cmp_ok(sprintf('%.2f', $auto->{$d}->{sub_titv_class}), q(==), 1.65, 'sub C2A titv class'); 
  cmp_ok(sprintf('%.2f', $auto->{$d}->{sub_titv_mean_ca}), q(==), 2.05, 'sub C2A titv mean'); 


};

subtest 'retrieve rna data' => sub {
  plan tests => 14;

  my $id_run = 24975;
  lives_ok {$schema_npg->resultset('Run')->update_or_create({folder_path_glob => $folder_glob, id_run => $id_run, })}
    'forder glob reset lives - test prerequisite';
  lives_ok {$schema_npg->resultset('Run')->find({id_run => $id_run, })->set_tag($user_id, 'staging')}
    'staging tag is set - test prerequisite';
  my $auto;
  lives_ok {$auto = npg_warehouse::loader::autoqc->new(autoqc_store => $store)->retrieve($id_run, $schema_npg)}
    'data for run 24975 retrieved';

  my $d = $compos_pkg->new(components =>
    [$compon_pkg->new(id_run => $id_run, position => 1, tag_index => 1)])->digest;
  cmp_ok(sprintf('%.10f',$auto->{$d}->{rna_exonic_rate}), q(==), 0.68215317, 'rna - exonic rate');
  cmp_ok(sprintf('%.10f',$auto->{$d}->{rna_genes_detected}), q(==), 12202, 'rna - genes detected');
  cmp_ok(sprintf('%.10f',$auto->{$d}->{rna_intronic_rate}), q(==), 0.27704784, 'rna - intronic rate');
  cmp_ok(sprintf('%.10f',$auto->{$d}->{rna_norm_3_prime_coverage}), q(==), 0.558965, 'rna - norm 3\' coverage');
  cmp_ok(sprintf('%.10f',$auto->{$d}->{rna_norm_5_prime_coverage}), q(==), 0.38012463, 'rna - norm 5\' coverage');
  cmp_ok(sprintf('%.10f',$auto->{$d}->{rna_percent_end_2_reads_sense}), q(==), 98.17338, 'rna - pct end 2 sense reads');
  cmp_ok(sprintf('%.10f',$auto->{$d}->{rna_rrna_rate}), q(==), 0.020362793, 'rna - rrna rate');
  cmp_ok(sprintf('%.10f',$auto->{$d}->{rna_transcripts_detected}), q(==), 71321, 'rna - transcripts detected');
  cmp_ok(sprintf('%.10f',$auto->{$d}->{rna_globin_percent_tpm}), q(==), 2.71, 'rna - globin percent tpm');
  cmp_ok(sprintf('%.10f',$auto->{$d}->{rna_mitochondrial_percent_tpm}), q(==), 6.56, 'rna - mitochondrial percent tpm');
  ok(! exists $auto->{$d}->{rna_rrna}, 'rna - rrna not present');
};

subtest 'retrieve gbs and generic data' => sub {
  plan tests => 58;

  my $id_run = 25710;
  lives_ok {$schema_npg->resultset('Run')->update_or_create({folder_path_glob => $folder_glob, id_run => $id_run, })}
    'folder glob reset lives - test prerequisite';
  lives_ok {$schema_npg->resultset('Run')->find({id_run => $id_run})->set_tag($user_id, 'staging')}
    'staging tag is set - test prerequisite';
  my $auto;
  lives_ok {$auto = npg_warehouse::loader::autoqc->new(
    autoqc_store => $store)->retrieve($id_run, $schema_npg)}
    'data for run 25710 retrieved';

  my $digest = $compos_pkg->new(components =>
    [$compon_pkg->new(id_run => $id_run, position => 1, tag_index => 60)])->digest;
  my $d = $auto->{$digest};
  # gbs
  cmp_ok(sprintf('%.10f',$d->{gbs_call_rate}), q(==), 1, 'gbs - call rate');
  cmp_ok(sprintf('%.10f',$d->{gbs_pass_rate}), q(==), 0.99, 'gbs - pass rate');
  # generic for artic
  my $pp_name = 'ncov2019-artic-nf';
  is($d->{pp}->{$pp_name}->{'pp_name'}, 'ncov2019-artic-nf', 'pp name');
  is($d->{pp}->{$pp_name}->{'pp_version'}, 'v0.10.0', 'pp version');
  is($d->{pp}->{$pp_name}->{'supplier_sample_name'}, 'XXXX-132', 'sample name');
  is($d->{pp}->{$pp_name}->{'artic_qc_outcome'}, 'FALSE', 'artic QC outcome');
  is($d->{pp}->{$pp_name}->{'pct_n_bases'}, '100.00', 'pct_n_bases');
  is($d->{pp}->{$pp_name}->{'num_aligned_reads'}, 2, 'num_aligned_reads');
  is($d->{pp}->{$pp_name}->{'pct_covered_bases'}, '0.00', 'pct_covered_bases');
  is($d->{pp}->{$pp_name}->{'longest_no_n_run'}, 1, 'longest_no_n_run');
  for my $name (qw/ivar_md ivar_amd pp_repo_url pct_n_bases_amd
                   longest_no_n_run_amd/) {
    ok (exists $d->{pp}->{$pp_name}->{$name}, "key $name exists");
    is ($d->{pp}->{$pp_name}->{$name}, undef, "$name is undefined");
  }
  # generic for ampliconstats
  my $ppa_name = 'ncov2019-artic-nf_ampliconstats';
  my $data_array = $d->{pp}->{$ppa_name};
  is (ref $data_array, 'ARRAY', 'array of data corresponds to a single sample');
  is (@{$data_array}, 98, 'length od the array is correct');
  my $expected = {
    'metric_fpcov_10' => '100.00',
    'metric_fpcov_20' => '100.00',
    'metric_freads' => '120466',
    'primer_panel_num_amplicons' => '98',
    'metric_fpcov_100' => '100.00',
    'metric_fpcov_1' => '100.00',
    'pp_name' => 'ncov2019-artic-nf_ampliconstats',
    'amplicon_index' => 1,
    'pp_version' => '1.0.0 1.11',
    'primer_panel' => 'nCoV-2019/V2/SARS-CoV-2/MN908947.3/nCoV-2019.bed'
  };
  is_deeply ($data_array->[0], $expected, 'first array member');
  $expected = {
    'metric_fpcov_10' => '49.03',
    'metric_fpcov_20' => '11.20',
    'metric_freads' => '40685',
    'primer_panel_num_amplicons' => '98',
    'metric_fpcov_100' => '0.00',
    'metric_fpcov_1' => '77.12',
    'pp_name' => 'ncov2019-artic-nf_ampliconstats',
    'amplicon_index' => 98,
    'pp_version' => '1.0.0 1.11',
    'primer_panel' => 'nCoV-2019/V2/SARS-CoV-2/MN908947.3/nCoV-2019.bed'
  };
  is_deeply ($data_array->[97], $expected, 'last array member');

  $digest = $compos_pkg->new(components =>
    [$compon_pkg->new(id_run => $id_run, position => 1, tag_index => 59)])->digest;
  $d = $auto->{$digest};
  # gbs
  ok (!defined $d->{gbs_call_rate}, 'gbs_call_rate not defined');
  ok (!defined $d->{gbs_pass_rate}, 'gbs_pass_rate not defined');
  # generic for artic
  is($d->{pp}->{$pp_name}->{pp_name}, 'ncov2019-artic-nf', 'pp name');
  is($d->{pp}->{$pp_name}->{pp_version}, 'v0.8.0', 'pp version');
  is($d->{pp}->{$pp_name}->{supplier_sample_name}, 'YYYY-131', 'sample name');
  is($d->{pp}->{$pp_name}->{artic_qc_outcome}, 'TRUE', 'artic QC outcome');
  is($d->{pp}->{$pp_name}->{pct_n_bases}, '0.40', 'pct_n_bases');
  is($d->{pp}->{$pp_name}->{num_aligned_reads}, 10773640, 'num_aligned_reads');
  is($d->{pp}->{$pp_name}->{pct_covered_bases}, '99.60', 'pct_covered_bases');
  is($d->{pp}->{$pp_name}->{longest_no_n_run}, 29783, 'longest_no_n_run');

  $data_array = $d->{pp}->{$ppa_name};
  is (ref $data_array, 'ARRAY', 'array of data corresponds to a single sample');
  is (@{$data_array}, 98, 'length of the array is correct');
  $expected = {
    'metric_fpcov_10' => '100.00',
    'metric_fpcov_20' => '100.00',
    'metric_freads' => '112541',
    'primer_panel_num_amplicons' => '98',
    'metric_fpcov_100' => '100.00',
    'metric_fpcov_1' => '100.00',
    'pp_name' => 'ncov2019-artic-nf_ampliconstats',
    'amplicon_index' => 1,
    'pp_version' => '1.0.0 1.11',
    'primer_panel' => 'nCoV-2019/V2/SARS-CoV-2/MN908947.3/nCoV-2019.bed'
  };
  is_deeply ($data_array->[0], $expected, 'first array member');
  $expected = {
    'metric_fpcov_10' => '100.00',
    'metric_fpcov_20' => '100.00',
    'metric_freads' => '60753',
    'primer_panel_num_amplicons' => '98',
    'metric_fpcov_100' => '100.00',
    'metric_fpcov_1' => '100.00',
    'pp_name' => 'ncov2019-artic-nf_ampliconstats',
    'amplicon_index' => 98,
    'pp_version' => '1.0.0 1.11',
    'primer_panel' => 'nCoV-2019/V2/SARS-CoV-2/MN908947.3/nCoV-2019.bed'
  };
  is_deeply ($data_array->[97], $expected, 'last array member');
  
  $digest = $compos_pkg->new(components =>
    [$compon_pkg->new(id_run => $id_run, position => 1, tag_index => 58)])->digest;
  $d = $auto->{$digest};
  ok (exists $d->{pp}->{$ppa_name}, 'data for astats pp exist');
  ok (! exists $d->{pp}->{$pp_name}, 'data for artic pp do not exist');

  $digest = $compos_pkg->new(components =>
    [$compon_pkg->new(id_run => $id_run, position => 1, tag_index => 57)])->digest;
  $d = $auto->{$digest};
  ok (! exists $d->{pp}->{$ppa_name}, 'data for astats pp do not exist');
  ok (exists $d->{pp}->{$pp_name}, 'data for artic pp exist');
  is($d->{pp}->{$pp_name}->{pp_name}, 'ncov2019-artic-nf', 'pp name');
  is($d->{pp}->{$pp_name}->{pp_version}, 'v1.3.0_wsi1.0', 'pp version');
  is($d->{pp}->{$pp_name}->{pp_repo_url},
  'https://github.com/wtsi-npg/ncov2019-artic-nf', 'pp URL');
  is($d->{pp}->{$pp_name}->{supplier_sample_name},
    'XYZQ-345678', 'sample name');
  is($d->{pp}->{$pp_name}->{artic_qc_outcome}, 'TRUE', 'artic QC outcome');
  is($d->{pp}->{$pp_name}->{pct_n_bases}, '1.32', 'pct_n_bases');
  is($d->{pp}->{$pp_name}->{pct_n_bases_amd}, '1.32', 'pct_n_bases_amd'); 
  is($d->{pp}->{$pp_name}->{num_aligned_reads}, 2655995, 'num_aligned_reads');
  is($d->{pp}->{$pp_name}->{pct_covered_bases}, '98.65', 'pct_covered_bases');
  is($d->{pp}->{$pp_name}->{longest_no_n_run}, 21663, 'longest_no_n_run');
  is($d->{pp}->{$pp_name}->{longest_no_n_run_amd}, 21663,
    'longest_no_n_run_amd');
  is($d->{pp}->{$pp_name}->{ivar_md}, 10, 'ivar_md');
  is($d->{pp}->{$pp_name}->{ivar_amd}, 100, 'ivar_amd');
};

subtest 'retrieve target stats data' => sub {
  plan tests => 16;

  my $id_run = 27116;
  lives_ok {$schema_npg->resultset('Run')->update_or_create({folder_path_glob => $folder_glob, id_run => $id_run, })}
    'folder glob reset lives - test prerequisite';
  lives_ok {$schema_npg->resultset('Run')->find({id_run => $id_run})->set_tag($user_id, 'staging')}
    'staging tag is set - test prerequisite';
  my $auto;
  lives_ok {$auto = npg_warehouse::loader::autoqc->new(autoqc_store => $store)->retrieve($id_run, $schema_npg)}
    'data for run 27116 retrieved';
 
  my $d = $compos_pkg->new(components =>
    [$compon_pkg->new(id_run => $id_run, position => 1, tag_index => 1)])->digest;
  is ($auto->{$d}->{target_filter}, 'F0xF04_target',  'target - target_filter');
  cmp_ok(sprintf('%.2f',$auto->{$d}->{target_coverage_threshold}), q(==), 15, 'target - target_coverage_threshold');
  cmp_ok(sprintf('%.2f',$auto->{$d}->{target_length}), q(==), '2945869055', 'target - target_length');
  cmp_ok(sprintf('%.2f',$auto->{$d}->{target_mapped_bases}), q(==), 8700474137, 'target - target_mapped_bases');
  cmp_ok(sprintf('%.2f',$auto->{$d}->{target_mapped_reads}), q(==), 58704583, 'target - target_mapped_reads');
  cmp_ok(sprintf('%.2f',$auto->{$d}->{target_percent_gt_coverage_threshold}), q(==), 0.15, 'target - target_percent_gt_coverage_threshold');
  cmp_ok(sprintf('%.2f',$auto->{$d}->{target_proper_pair_mapped_reads}), q(==), 57355728, 'target - target_proper_pair_mapped_reads');

  is ($auto->{$d}->{target_autosome_filter}, 'F0xF04_target_autosome',  'target - target_autosome_filter');
  cmp_ok(sprintf('%.2f',$auto->{$d}->{target_autosome_length}), q(==), '2762956253', 'target - target_autosome_length');
  cmp_ok(sprintf('%.2f',$auto->{$d}->{target_autosome_mapped_bases}), q(==), 8282053108, 'target - target_autosome_mapped_bases');
  cmp_ok(sprintf('%.2f',$auto->{$d}->{target_autosome_coverage_threshold}), q(==), 15, 'target - target_autosome_coverage_threshold');
  cmp_ok(sprintf('%.2f',$auto->{$d}->{target_autosome_percent_gt_coverage_threshold}), q(==), 0.17, 'target - target_autosome_percent_gt_coverage_threshold');
  cmp_ok(sprintf('%.2f',$auto->{$d}->{nrd_percent}), q(==), 0.00, 'nrd');
};

subtest 'retrieve data for multi-component compositions' => sub {
  plan tests => 19;

  my $id_run = 26291;
  $schema_npg->resultset('Run')->update_or_create({
    folder_path_glob     => $folder_glob,
    id_run               => $id_run,
    folder_name          => 'with_merges',
    id_instrument_format => 10,
    team                 => 'A'});
  $schema_npg->resultset('Run')
             ->find({id_run => $id_run})->set_tag($user_id, 'staging');

  throws_ok { npg_warehouse::loader::autoqc->new(autoqc_store => $store)
                                           ->retrieve($id_run, $schema_npg) }
    qr/Interop column names should be set/, 'errow when interop column names are not set';
 
  my @column_names =   qw/
    cluster_count_mean
    cluster_count_pf_mean
    cluster_count_pf_stdev
    cluster_count_pf_total
    cluster_count_stdev
    cluster_count_total
    cluster_density_mean
    cluster_density_pf_mean
    cluster_density_pf_stdev
    cluster_density_stdev
    cluster_pf_mean
    cluster_pf_stdev
  /;

  my $auto = npg_warehouse::loader::autoqc->new(
     autoqc_store => $store,
     interop_data_column_names => [map { 'interop_' . $_ } @column_names]
  )->retrieve($id_run, $schema_npg);

  my $expected = {
           '1' => {
                   "interop_cluster_count_mean" =>  4091904,
                   "interop_cluster_count_pf_mean" =>  3121136.52840909,
                   "interop_cluster_count_pf_stdev" =>  35124.2526587081,
                   "interop_cluster_count_pf_total" =>  2197280116,
                   "interop_cluster_count_stdev" =>  0,
                   "interop_cluster_count_total" =>  2880700416,
                   "interop_cluster_density_mean" =>  2961263.95700836,
                   "interop_cluster_density_pf_mean" =>  2258730.68050473,
                   "interop_cluster_density_pf_stdev" =>  25419.018485059,
                   "interop_cluster_density_stdev" =>  4.65992365406662e-09,
                   "interop_cluster_pf_mean" =>  76.2758981737863,
                   "interop_cluster_pf_stdev" =>  0.85838408375925,
                   'tag_hops_power' => 1,
                   'ref_match2_name' => 'Pan troglodytes CHIMP2.1.4',
                   'tags_decode_cv' => '15.14',
                   'tag_hops_percent' => '1.431923',
                   'ref_match1_percent' => '95.5',
                   'ref_match2_percent' => '89.1',
                   'ref_match1_name' => 'Homo sapiens 1000Genomes',
                   'tags_decode_percent' => '93.77'},
           '1:7' => {'tag_decode_percent' => '6.90',
                   'tag_sequence' => 'ACACATTC-CTGTCGGT',
                   'tag_decode_count' => '68726651'},
           '1:9' => {'tag_decode_count' => '110359514',
                   'tag_decode_percent' => '11.10',
                   'tag_sequence' => 'ACAGCCTT-CACACGCG'},
           '1:12' => {'tag_decode_count' => '66558340',
                    'tag_sequence' => 'TTGCCATC-ACGCGTCA',
                    'tag_decode_percent' => '6.70'},
           '1:5' => {'tag_decode_percent' => '8',
                   'tag_sequence' => 'ACACACCT-GGCAACTG',
                   'tag_decode_count' => '79299831' },
           '1:11' => {'tag_decode_percent' => '7.80',
                    'tag_sequence' => 'ACAGGCAG-AGCAAGTT',
                    'tag_decode_count' => '77788454'},
           '1:888' => {'tag_sequence' => 'ACAACGCA-TCTTTCCC',
                     'tag_decode_percent' => '0',
                     'tag_decode_count' => '9'},
           '1:3' => {'tag_decode_percent' => '8.70',
                   'tag_sequence' => 'ACACTAAC-ACAGTGAA',
                   'tag_decode_count' => '86434858'},
           '1:6' => {'tag_decode_count' => '63169296',
                   'tag_sequence' => 'GGTGTCCG-CAGCGTCT',
                   'tag_decode_percent' => '6.30'},
           '1:0' => {'tag_decode_count' => '62102003',
                    'tag_decode_percent' => '6.20'},
           '1:10' => {'tag_sequence' => 'TTGCAGTA-AGCCAACA',
                    'tag_decode_percent' => '7.10',
                    'tag_decode_count' => '70535174'},
           '1:8' => {'tag_sequence' => 'TTGCTTAA-TCGGCGTT',
                   'tag_decode_percent' => '7.30',
                   'tag_decode_count' => '72387564'},
           '1:1' => {'tag_decode_percent' => '8.10',
                   'tag_sequence' => 'AACACATA-AATTCTAA',
                   'tag_decode_count' => '81073574'},
           '1:4' => {'tag_decode_count' => '81708200',
                   'tag_decode_percent' => '8.20',
                   'tag_sequence' => 'TTGTGTTC-AGATGTGA'},
           '1:2' => {'tag_sequence' => 'TGGTGTCT-TTCATCTG',
                   'tag_decode_percent' => '7.70',
                   'tag_decode_count' => '76726441'}
  };
 
  while (my ($key, $value) = each %{$expected}) {
    my ($p, $ti) = split /:/, $key;
    my $init = {id_run => $id_run, position => $p};
    if (defined $ti) {
      $init->{tag_index} = $ti;
    }
    my $d = $compos_pkg->new(components => [$compon_pkg->new($init)])->digest;
    delete $auto->{$d}->{composition};
    is_deeply ($auto->{$d}, $expected->{$key}, "correct data for $key");
  } 

  my @components = 
    map {$compon_pkg->new($_)}
    map { {id_run => $id_run, position => $_, tag_index => 1} }
    qw/1 2/;
  my $d = $compos_pkg->new(components => \@components)->digest;
  ok (exists $auto->{$d}, 'data for tag 1 merge exists');
  my $c = delete $auto->{$d}->{'composition'};
  is ($c->digest, $d, 'composition digest is correct');
  $expected =  {'insert_size_median' => 498,
                'insert_size_quartile1' => 420,
                'insert_size_num_modes' => 2,
                'insert_size_quartile3' => 611,
                'insert_size_normal_fit_confidence' => '0.64'};
  is_deeply (delete $auto->{$d}, $expected, 'insert size data are retrieved');
};

1;
