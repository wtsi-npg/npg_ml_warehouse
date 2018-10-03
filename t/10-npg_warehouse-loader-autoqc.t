use strict;
use warnings;
use Test::More tests => 128;
use Test::Exception;
use Moose::Meta::Class;

use npg_testing::db;
use npg_qc::autoqc::qc_store;
use npg_qc::autoqc::results::qX_yield;

use_ok('npg_warehouse::loader::autoqc');
my $store = npg_qc::autoqc::qc_store->new(use_db => 0);
my $plex_key = q[plexes];
my $util = Moose::Meta::Class->create_anon_class(
    roles => [qw/npg_testing::db/])->new_object({});
my $schema_npg  = $util->create_test_db(q[npg_tracking::Schema], q[t/data/fixtures/npg]);
my $folder_glob = q[t/data/runfolders/];
my $user_id = 7;

{
  throws_ok {npg_warehouse::loader::autoqc->new(autoqc_store => $store)}
    qr/Attribute \(plex_key\) is required/,
    'error in constructor when plex key attr is not defined';

  my $autoqc;
  lives_ok {
    $autoqc = npg_warehouse::loader::autoqc->new( 
                    autoqc_store => $store,
                    plex_key => $plex_key
                )
  } 'autoqc retriever object instantiated by passing schema objects to the constructor';
  isa_ok ($autoqc, 'npg_warehouse::loader::autoqc');
  is ($autoqc->verbose, 0, 'verbose mode is off');
  is ($autoqc->plex_key, $plex_key, 'plex_key attr set');
}

{
  my $autoqc  = npg_warehouse::loader::autoqc->new( 
                    autoqc_store => npg_qc::autoqc::qc_store->new(use_db => 0),
                    plex_key => $plex_key,);
  throws_ok {$autoqc->retrieve()}
    qr/Attribute \(id_run\) does not pass the type constraint/,
    'error when id_run is missing';
}

{
   my $autoqc  = npg_warehouse::loader::autoqc->new( 
                    autoqc_store => $store,
                    plex_key => $plex_key
                 );
   my $result = npg_qc::autoqc::results::qX_yield->new(
     path => q[dodo], id_run=>22, position=>1, threshold_quality => 30,);
   throws_ok {$autoqc->_qX_yield($result, {})}
     qr/Need Q20 quality, got 30/, 'error when quality is not 20';
   $result->threshold_quality(20);
   lives_ok {$autoqc->_qX_yield($result, {})} 'lives with quality 20';
   $result->yield1(200);
   my $h = {};
   $autoqc->_qX_yield($result, $h);
   is ($h->{1}->{q20_yield_kb_forward_read}, 200, 'retrieved q20 for the forward read');
   ok(!exists $h->{1}->{q20_yield_kb_reverse_read}, 'reverse slot does not exist');
   $result->yield2(300);
   $autoqc->_qX_yield($result, $h);
   is ($h->{1}->{q20_yield_kb_reverse_read}, 300, 'reverse slot filled');
   $result->tag_index(3);
   $autoqc->_qX_yield($result, $h);
   is ($h->{1}->{$plex_key}->{3}->{q20_yield_kb_forward_read}, 200, 'tag forward slot filled');
   is ($h->{1}->{$plex_key}->{3}->{q20_yield_kb_reverse_read}, 300, 'tag reverse slot filled');
   $result->tag_index(0);
   $autoqc->_qX_yield($result, $h);
   is ($h->{1}->{$plex_key}->{0}->{q20_yield_kb_forward_read}, 200, 'zero tag forward slot filled');
   is ($h->{1}->{$plex_key}->{0}->{q20_yield_kb_reverse_read}, 300, 'zero tag reverse slot filled');
}

{
  my $id_run = 4799;
  lives_ok {$schema_npg->resultset('Run')->find({id_run => $id_run})->set_tag($user_id, 'staging')}
    'staging tag is set - test prerequisite';
  lives_ok {$schema_npg->resultset('Run')
    ->update_or_create({folder_path_glob => $folder_glob, folder_name => '100330_HS21_4799', id_run => $id_run, })}
    'forder glob reset lives - test prerequisite';

  my $auto;
  lives_ok { $auto = npg_warehouse::loader::autoqc->new( 
             autoqc_store => $store,
             plex_key => $plex_key )->retrieve($id_run, $schema_npg)}
    'data for run 4799 retrieved';

  is ($auto->{1}->{q20_yield_kb_forward_read}, 46671, 'qx forward lane 1');
  is ($auto->{1}->{q20_yield_kb_reverse_read}, 39877, 'qx reverse lane 1');

  is ($auto->{1}->{ref_match1_name},  q[Homo sapiens 1000Genomes], 'ref_match name1 lane 1');
  is ($auto->{1}->{ref_match1_percent}, 95.7, 'ref_match count1 lane 1');
  is ($auto->{1}->{ref_match2_name},  q[Gorilla gorilla gorilla], 'ref_match name2 lane 1');
  is ($auto->{1}->{ref_match2_percent}, 85.2, 'ref_match count2 lane 1');
  is ($auto->{4}->{ref_match1_name},  q[Homo sapiens 1000Genomes], 'ref_match name1 lane 4');
  is ($auto->{4}->{ref_match1_percent}, 97.2, 'ref_match count1 lane 4');
  is ($auto->{4}->{ref_match2_name},  q[Gorilla gorilla gorilla], 'ref_match name2 lane 4');
  is ($auto->{4}->{ref_match2_percent}, 87.2, 'ref_match count2 lane 4');

  ok (!exists  $auto->{4}->{$plex_key}, 'autoqc plex hash for lane 4 does not exist');
  ok (!exists  $auto->{1}->{$plex_key}, 'autoqc plex hash for lane 1 does not exist');
  ok (exists  $auto->{3}->{$plex_key}, 'autoqc plex hash for lane 3 exists');

  is ($auto->{3}->{$plex_key}->{0}->{q20_yield_kb_forward_read}, 46671, 'qx reverse tag 0');
  is ($auto->{3}->{$plex_key}->{0}->{q20_yield_kb_reverse_read}, 39877, 'qx reverse tag 0');
  is ($auto->{3}->{$plex_key}->{3}->{q20_yield_kb_forward_read}, 1455655, 'qx forward tag 3');
  is ($auto->{3}->{$plex_key}->{3}->{q20_yield_kb_reverse_read}, 1393269, 'qx reverse tag 3');
}

{
  my $id_run = 4333;
  lives_ok {$schema_npg->resultset('Run')->find({id_run => $id_run})->set_tag($user_id, 'staging')}
    'staging tag is set - test prerequisite';
  lives_ok {$schema_npg->resultset('Run')
    ->update_or_create({folder_path_glob => $folder_glob, folder_name => '100330_IL21_4333', id_run => $id_run, })}
    'forder glob reset lives - test prerequisite';
  my $auto;
  lives_ok {$auto = npg_warehouse::loader::autoqc->new( 
                    autoqc_store => $store,
                    plex_key => $plex_key
  )->retrieve($id_run, $schema_npg)} 'data for run 4333 retrieved';

  ok (exists  $auto->{4}, 'control lane present in an autoqc hash');
  ok (!exists  $auto->{4}->{$plex_key}, 'control lane not present in an autoqc plex hash');
  is ($auto->{4}->{contaminants_scan_hit1_name}, 'PhiX',   'contam check value');
  is ($auto->{4}->{contaminants_scan_hit1_score}, '97.30',   'contam check value');
  is ($auto->{4}->{contaminants_scan_hit2_name}, 'Mus_musculus',   'contam check value');
  is ($auto->{4}->{contaminants_scan_hit2_score}, '0.12',   'contam check value');
  is ($auto->{4}->{insert_size_quartile1}, 172,   'insert size q1');
  is ($auto->{4}->{insert_size_quartile3}, 207,   'insert size q3');
  is ($auto->{4}->{insert_size_median},    189,   'insert size median');
  is ($auto->{4}->{insert_size_num_modes},             1,     'insert size num modes');
  is ($auto->{4}->{insert_size_normal_fit_confidence}, '0.35',   'insert size norm fit conf');

  ok (exists  $auto->{1}->{$plex_key}, 'lane 1 present in an autoqc plex hash');
  ok (exists  $auto->{1}->{$plex_key}->{3}, 'lane 1 tag index 3 present in an autoqc plex hash');
  ok (exists  $auto->{1}->{$plex_key}->{1}, 'lane 1 tag index 1 present in an autoqc plex hash');
  ok (exists  $auto->{1}->{$plex_key}->{6}, 'lane 1 tag index 6 present in an autoqc plex hash');
  is ($auto->{1}->{$plex_key}->{1}->{tag_sequence}, 'ATCACG', 'lane 1 tag index 1 tag sequence');
  is ($auto->{1}->{$plex_key}->{6}->{tag_decode_percent}, 3.49, 'lane 1 tag index 6 tag decode percent');
  is ($auto->{1}->{$plex_key}->{6}->{tag_decode_count}, 1111591, 'lane 1 tag index 6 tag decode count');
  cmp_ok(sprintf('%.2f', $auto->{1}->{tags_decode_percent}), q(==), 99.29, 'lane 1 tag decode percent from tag decode stats');
  cmp_ok(sprintf('%.2f', $auto->{1}->{tags_decode_cv}), q(==), 55.1, 'lane 1 tag coeff of var from tag decode stats');
}

{
  my $id_run = 6624;
  lives_ok {$schema_npg->resultset('Run')->find({id_run => $id_run})->set_tag($user_id, 'staging')}
    'staging tag is set - test prerequisite';
  lives_ok {$schema_npg->resultset('Run')
    ->update_or_create({folder_path_glob => $folder_glob, folder_name => '110731_HS17_06624_A_B00T5ACXX', id_run => $id_run, })}
    'forder glob reset lives - test prerequisite';

  my $auto;
  lives_ok {$auto = npg_warehouse::loader::autoqc->new(
                    autoqc_store => $store,
                    plex_key => $plex_key
  )->retrieve($id_run, $schema_npg);} 'data for run 6624 retrieved';

  cmp_ok(sprintf('%.2f', $auto->{3}->{split_phix_percent}), q(==), 0.44, 'split phiX percent');
  cmp_ok(sprintf('%.2f', $auto->{3}->{$plex_key}->{4}->{bam_num_reads}), q(==), 33605036, 'bam number of reads');
  cmp_ok(sprintf('%.2f', $auto->{3}->{$plex_key}->{4}->{bam_percent_mapped}), q(==), 96.12, 'bam mapped percent');
  cmp_ok(sprintf('%.2f', $auto->{3}->{$plex_key}->{4}->{bam_percent_duplicate}), q(==), 1.04, 'bam duplicate percent');

  is($auto->{3}->{$plex_key}->{1}->{genotype_sample_name_match}, '23/25', 'gt_sample_name_match check');
  is($auto->{3}->{$plex_key}->{1}->{genotype_sample_name_relaxed_match}, '24/25', 'gt_sample_name_relaxed_match check');
  cmp_ok(sprintf('%.2f', $auto->{3}->{$plex_key}->{1}->{genotype_mean_depth}), q(==), 50.12, 'genotype_mean_depth check');

  cmp_ok(sprintf('%.2f', $auto->{3}->{$plex_key}->{1}->{mean_bait_coverage}), q(==), 41.49, 'mean bait coverage');
  cmp_ok(sprintf('%.2f', $auto->{3}->{$plex_key}->{1}->{on_bait_percent}), q(==), 68.06, 'on bait percent');
  cmp_ok(sprintf('%.2f', $auto->{3}->{$plex_key}->{1}->{on_or_near_bait_percent}), q(==), 88.92, 'on or near bait percent');
  cmp_ok(sprintf('%.2f', $auto->{3}->{$plex_key}->{2}->{mean_bait_coverage}), q(==), 42.64, 'mean bait coverage');
  cmp_ok(sprintf('%.2f', $auto->{3}->{$plex_key}->{2}->{on_bait_percent}), q(==), 68.26, 'on bait percent');
  cmp_ok(sprintf('%.2f', $auto->{3}->{$plex_key}->{2}->{on_or_near_bait_percent}), q(==), 89.09, 'on or near bait percent'); 

  cmp_ok(sprintf('%.2f', $auto->{1}->{tags_decode_percent}), q(==), 98.96, 'lane 1 tag decode percent from tag metrics');
  cmp_ok(sprintf('%.2f', $auto->{1}->{tag_hops_percent}), q(==), 1.23, 'lane 1 percent tag hops from tag metrics');
  cmp_ok(sprintf('%.2f', $auto->{1}->{tag_hops_power}), q(==), 0.85, 'lane 1 tag hops power from tag metrics');
  cmp_ok(sprintf('%.2f', $auto->{1}->{tags_decode_cv}), q(==), 11.78, 'lane 1 tag decode coeff of var from tag metrics');
  cmp_ok(sprintf('%.2f', $auto->{2}->{tags_decode_percent}), q(==), 98.96, 'lane 2 tag decode percent from tag metrics in presence of tag decode stats');
  cmp_ok(sprintf('%.2f', $auto->{2}->{tags_decode_cv}), q(==), 11.69, 'lane 2 tag matrics coeff of var from tag metrics in presence of tag decode stats');
  cmp_ok(sprintf('%.2f', $auto->{3}->{tags_decode_percent}), q(==), 99.05, 'lane 3 tag decode percent from tag decode stats in absence of tag metrics');
  cmp_ok(sprintf('%.2f', $auto->{4}->{tags_decode_percent}), q(==), 98.96, 'lane 4 tag decode percent from tag metrics');
  cmp_ok(sprintf('%.2f', $auto->{5}->{tags_decode_percent}), q(==), 98.98, 'lane 5 tag decode percent from tag metrics');

  is ($auto->{1}->{$plex_key}->{1}->{tag_sequence}, 'ATCACGTT', 'lane 1 tag index 1 tag sequence');
  ok (!exists $auto->{1}->{$plex_key}->{0}->{tag_sequence}, 'index zero tag sequence is not defined');
  is ($auto->{1}->{$plex_key}->{0}->{tag_decode_count}, 1831358, 'lane 1 tag index 0 count');
  is ($auto->{2}->{$plex_key}->{168}->{tag_sequence}, 'ACAACGCA', 'lane 2 tag index 168 tag sequence');
  is ($auto->{2}->{$plex_key}->{168}->{tag_decode_count}, 1277701, 'lane 2 tag index 168 count');
  cmp_ok (sprintf('%.2f', $auto->{2}->{$plex_key}->{168}->{tag_decode_percent}), q(==), 0.73, , 'lane 2 tag index 168 percent');
}

{
  my $id_run = 6642;
  lives_ok {$schema_npg->resultset('Run')->update_or_create({folder_path_glob => $folder_glob, id_run => $id_run, })}
    'forder glob reset lives - test prerequisite';
  lives_ok {$schema_npg->resultset('Run')->find({id_run => $id_run})->set_tag($user_id, 'staging')}
    'staging tag is set - test prerequisite';

  my $auto = npg_warehouse::loader::autoqc->new(autoqc_store => $store,plex_key => $plex_key)->retrieve($id_run, $schema_npg);

  cmp_ok(sprintf('%.5f',$auto->{4}->{verify_bam_id_score}), q(==), 0.00166, 'verify_bam_id_score');
  cmp_ok(sprintf('%.2f',$auto->{4}->{verify_bam_id_average_depth}), q(==), 9.42, 'verify_bam_id_average_depth');
  cmp_ok($auto->{4}->{verify_bam_id_snp_count}, q(==), 1531960, 'verify_bam_id_snp_count');

  cmp_ok(sprintf('%.2f',$auto->{3}->{bam_percent_mapped}), q(==), 98.19, 'bam mapped percent');
  cmp_ok(sprintf('%.2f',$auto->{3}->{bam_percent_duplicate}), q(==), 24.63, 'bam duplicate percent');
  ok(! exists $auto->{1}->{split_human_percent}, 'split human percent not present');
  cmp_ok(sprintf('%.2f',$auto->{2}->{split_human_percent}), q(==), 0.18, 'split human percent');
  cmp_ok(sprintf('%.2f',$auto->{2}->{$plex_key}->{4}->{bam_human_percent_mapped}), q(==), 55.3, 'bam human mapped percent');
  cmp_ok(sprintf('%.2f',$auto->{2}->{$plex_key}->{4}->{bam_human_percent_duplicate}), q(==), 68.09, 'bam human duplicate percent');
  cmp_ok(sprintf('%.2f',$auto->{2}->{$plex_key}->{4}->{bam_num_reads}), q(==), 138756624, 'bam (nonhuman) number of reads');
  cmp_ok(sprintf('%.2f',$auto->{2}->{$plex_key}->{4}->{bam_percent_mapped}), q(==), 96.3, 'bam (nonhuman) mapped percent');
  cmp_ok(sprintf('%.2f',$auto->{2}->{$plex_key}->{4}->{bam_percent_duplicate}), q(==), 6.34, 'bam (nonhuman) duplicate percent');
  cmp_ok(sprintf('%.2f',$auto->{2}->{$plex_key}->{5}->{bam_human_percent_mapped}), q(==), 55.3, 'bam xahuman mapped percent as human');
  cmp_ok(sprintf('%.2f',$auto->{2}->{$plex_key}->{5}->{bam_human_percent_duplicate}), q(==), 68.09, 'bam xahuman duplicate percent as human');
  cmp_ok(sprintf('%.2f',$auto->{2}->{$plex_key}->{6}->{bam_human_percent_mapped}), q(==), 55.3, 'bam yhuman mapped percent as human');
  cmp_ok(sprintf('%.2f',$auto->{2}->{$plex_key}->{6}->{bam_human_percent_duplicate}), q(==), 68.09, 'bam yhuman duplicate percent as human');
}

{
  my $id_run = 24975;
  lives_ok {$schema_npg->resultset('Run')->update_or_create({folder_path_glob => $folder_glob, id_run => $id_run, })}
    'forder glob reset lives - test prerequisite';
  lives_ok {$schema_npg->resultset('Run')->find({id_run => $id_run, })->set_tag($user_id, 'staging')}
    'staging tag is set - test prerequisite';
  my $auto;
  lives_ok {$auto = npg_warehouse::loader::autoqc->new(autoqc_store => $store, plex_key => $plex_key )->retrieve($id_run, $schema_npg)}
    'data for run 24975 retrieved';
  cmp_ok(sprintf('%.10f',$auto->{1}->{$plex_key}->{1}->{rna_exonic_rate}), q(==), 0.68215317, 'rna - exonic rate');
  cmp_ok(sprintf('%.10f',$auto->{1}->{$plex_key}->{1}->{rna_genes_detected}), q(==), 12202, 'rna - genes detected');
  cmp_ok(sprintf('%.10f',$auto->{1}->{$plex_key}->{1}->{rna_intronic_rate}), q(==), 0.27704784, 'rna - intronic rate');
  cmp_ok(sprintf('%.10f',$auto->{1}->{$plex_key}->{1}->{rna_norm_3_prime_coverage}), q(==), 0.558965, 'rna - norm 3\' coverage');
  cmp_ok(sprintf('%.10f',$auto->{1}->{$plex_key}->{1}->{rna_norm_5_prime_coverage}), q(==), 0.38012463, 'rna - norm 5\' coverage');
  cmp_ok(sprintf('%.10f',$auto->{1}->{$plex_key}->{1}->{rna_percent_end_2_reads_sense}), q(==), 98.17338, 'rna - pct end 2 sense reads');
  cmp_ok(sprintf('%.10f',$auto->{1}->{$plex_key}->{1}->{rna_rrna_rate}), q(==), 0.020362793, 'rna - rrna rate');
  cmp_ok(sprintf('%.10f',$auto->{1}->{$plex_key}->{1}->{rna_transcripts_detected}), q(==), 71321, 'rna - transcripts detected');
  cmp_ok(sprintf('%.10f',$auto->{1}->{$plex_key}->{1}->{rna_globin_percent_tpm}), q(==), 2.71, 'rna - globin percent tpm');
  cmp_ok(sprintf('%.10f',$auto->{1}->{$plex_key}->{1}->{rna_mitochondrial_percent_tpm}), q(==), 6.56, 'rna - mitochondrial percent tpm');
  ok(! exists $auto->{1}->{$plex_key}->{1}->{rna_rrna}, 'rna - rrna not present');
}

{
  my $id_run = 25710;
  lives_ok {$schema_npg->resultset('Run')->update_or_create({folder_path_glob => $folder_glob, id_run => $id_run, })}
    'folder glob reset lives - test prerequisite';
  lives_ok {$schema_npg->resultset('Run')->find({id_run => $id_run})->set_tag($user_id, 'staging')}
    'staging tag is set - test prerequisite';
  my $auto;
  lives_ok {$auto = npg_warehouse::loader::autoqc->new(autoqc_store => $store, plex_key => $plex_key )->retrieve($id_run, $schema_npg)}
    'data for run 25710 retrieved';

  cmp_ok(sprintf('%.10f',$auto->{1}->{$plex_key}->{60}->{gbs_call_rate}), q(==), 1, 'gbs - call rate');
  cmp_ok(sprintf('%.10f',$auto->{1}->{$plex_key}->{60}->{gbs_pass_rate}), q(==), 0.99, 'gbs - pass rate');
}

{
  $schema_npg->resultset('Run')->update_or_create({
    folder_path_glob     => $folder_glob,
    id_run               => 26291,
    folder_name          => 'with_merges',
    id_instrument_format => 10,
    team                 => 'A'});
  $schema_npg->resultset('Run')->find({id_run => 26291})->set_tag($user_id, 'staging');
  my $auto = npg_warehouse::loader::autoqc->new(autoqc_store => $store, plex_key => $plex_key)
                                          ->retrieve(26291, $schema_npg);

  my $expected = {
    '1' => {
        'tag_hops_power' => 1,
        'ref_match2_name' => 'Pan troglodytes CHIMP2.1.4',
        'tags_decode_cv' => '15.14',
        'tag_hops_percent' => '1.431923',
        'ref_match1_percent' => '95.5',
        'ref_match2_percent' => '89.1',
        'ref_match1_name' => 'Homo sapiens 1000Genomes',
        'tags_decode_percent' => '93.77',
        'plexes' => {
           '7' => {'tag_decode_percent' => '6.90',
                   'tag_sequence' => 'ACACATTC-CTGTCGGT',
                   'tag_decode_count' => '68726651'},
           '9' => {'tag_decode_count' => '110359514',
                   'tag_decode_percent' => '11.10',
                   'tag_sequence' => 'ACAGCCTT-CACACGCG'},
           '12' => {'tag_decode_count' => '66558340',
                    'tag_sequence' => 'TTGCCATC-ACGCGTCA',
                    'tag_decode_percent' => '6.70'},
           '5' => {'tag_decode_percent' => '8',
                   'tag_sequence' => 'ACACACCT-GGCAACTG',
                   'tag_decode_count' => '79299831' },
           '11' => {'tag_decode_percent' => '7.80',
                    'tag_sequence' => 'ACAGGCAG-AGCAAGTT',
                    'tag_decode_count' => '77788454'},
           '888' => {'tag_sequence' => 'ACAACGCA-TCTTTCCC',
                     'tag_decode_percent' => '0',
                     'tag_decode_count' => '9'},
           '3' => {'tag_decode_percent' => '8.70',
                   'tag_sequence' => 'ACACTAAC-ACAGTGAA',
                   'tag_decode_count' => '86434858'},
           '6' => {'tag_decode_count' => '63169296',
                   'tag_sequence' => 'GGTGTCCG-CAGCGTCT',
                   'tag_decode_percent' => '6.30'},
           '0' => {'tag_decode_count' => '62102003',
                    'tag_decode_percent' => '6.20'},
           '10' => {'tag_sequence' => 'TTGCAGTA-AGCCAACA',
                    'tag_decode_percent' => '7.10',
                    'tag_decode_count' => '70535174'},
           '8' => {'tag_sequence' => 'TTGCTTAA-TCGGCGTT',
                   'tag_decode_percent' => '7.30',
                   'tag_decode_count' => '72387564'},
           '1' => {'tag_decode_percent' => '8.10',
                   'tag_sequence' => 'AACACATA-AATTCTAA',
                   'tag_decode_count' => '81073574'},
           '4' => {'tag_decode_count' => '81708200',
                   'tag_decode_percent' => '8.20',
                   'tag_sequence' => 'TTGTGTTC-AGATGTGA'},
           '2' => {'tag_sequence' => 'TGGTGTCT-TTCATCTG',
                   'tag_decode_percent' => '7.70',
                   'tag_decode_count' => '76726441'}
     },}
  };
 
  is_deeply ($auto, $expected, 'lane-level results in, multi-component plex skipped'); 
} 

1;
