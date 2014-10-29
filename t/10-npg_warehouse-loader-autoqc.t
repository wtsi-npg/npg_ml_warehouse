#########
# Author:        Marina Gourtovaia
# Maintainer:    $Author: mg8 $
# Created:       4 August 2010
# Last Modified: $Date: 2010-08-03 18:04:59 +0100 (Tue, 03 Aug 2010) $
# Id:            $Id: 20-npg_warehouse-loader.t 10401 2010-08-03 17:04:59Z mg8 $
# $HeadURL: svn+ssh://svn.internal.sanger.ac.uk/repos/svn/new-pipeline-dev/data_handling/branches/prerelease-24.0/t/20-npg_warehouse-loader.t $
#

use strict;
use warnings;
use Test::More tests => 92;
use Test::Exception;
use t::npg_warehouse::util;

use npg_qc::autoqc::qc_store;
use npg_qc::autoqc::results::qX_yield;

# set up the location of the test staging area
my $test_dir = q[t/data/archive];
local $ENV{TEST_DIR} = $test_dir;

use_ok('npg_warehouse::loader::autoqc');

my $plex_key = q[plexes];

{
  my $autoqc;
  lives_ok {
      $autoqc = npg_warehouse::loader::autoqc->new( 
                    autoqc_store => npg_qc::autoqc::qc_store->new(use_db => 0),
                    plex_key => $plex_key
                )
  } 'autoqc retriever object instantiated by passing schema objects to the constructor';
  isa_ok ($autoqc, 'npg_warehouse::loader::autoqc');
  is ($autoqc->verbose, 0, 'verbose mode is off');
  is ($autoqc->plex_key, $plex_key, 'plex_key attr set');
}


{
  throws_ok {npg_warehouse::loader::autoqc->new( 
                    autoqc_store => npg_qc::autoqc::qc_store->new(use_db => 0)
  )} qr/Attribute \(plex_key\) is required/, 'error in constructor when plex key attr is not defined';
}


{
  my $autoqc  = npg_warehouse::loader::autoqc->new( 
                    autoqc_store => npg_qc::autoqc::qc_store->new(use_db => 0,verbose => 0,),
                    plex_key => $plex_key,
                );
  throws_ok {$autoqc->retrieve()} qr/Attribute \(id_run\) does not pass the type constraint/, 'error when id_run is missing';
  lives_ok {$autoqc->retrieve(1)} 'lives when id_run is one';
}


{
   my $autoqc  = npg_warehouse::loader::autoqc->new( 
                    autoqc_store => npg_qc::autoqc::qc_store->new(use_db => 0),
                    plex_key => $plex_key
                 );
   my $result = npg_qc::autoqc::results::qX_yield->new(path => q[dodo], id_run=>22, position=>1,
                                                     threshold_quality => 30,);
   throws_ok {$autoqc->_qX_yield($result, {})} qr/Need Q20 quality, got 30/, 'error when quality is not 20';
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
  my $autoqc  = npg_warehouse::loader::autoqc->new( 
                    autoqc_store => npg_qc::autoqc::qc_store->new(use_db => 0),
                    plex_key => $plex_key
                );

  my $id_run = 4799;
  my $auto = $autoqc->retrieve($id_run);
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
  my $autoqc  = npg_warehouse::loader::autoqc->new( 
                    autoqc_store => npg_qc::autoqc::qc_store->new(use_db => 0),
                    plex_key => $plex_key
                );
  my $id_run = 4333;
  my $auto = $autoqc->retrieve($id_run);

  ok (exists  $auto->{4}, 'control lane present in an autoqc hash');
  ok (!exists  $auto->{4}->{$plex_key}, 'control lane not present in an autoqc plex hash');
  is ($auto->{4}->{contaminants_scan_hit1_name}, 'PhiX',   'contam check value');
  is ($auto->{4}->{contaminants_scan_hit1_score}, '97.30',   'contam check value');
  is ($auto->{4}->{contaminants_scan_hit2_name}, 'Mus_musculus',   'contam check value');
  is ($auto->{4}->{contaminants_scan_hit2_score}, '0.10',   'contam check value');

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
  my $autoqc  = npg_warehouse::loader::autoqc->new(
                    autoqc_store => npg_qc::autoqc::qc_store->new(use_db => 0),
                    plex_key => $plex_key
                );
  my $id_run = 6624;
  my $auto = $autoqc->retrieve($id_run);

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
  my $autoqc  = npg_warehouse::loader::autoqc->new(
                    autoqc_store => npg_qc::autoqc::qc_store->new(use_db => 0),
                    plex_key => $plex_key
                );
  my $id_run = 6642;
  my $schema_npg;
  my $util = t::npg_warehouse::util->new();
  eval { $schema_npg  = $util->create_test_db(q[npg_tracking::Schema], q[t/data/fixtures/npg]); 1 } or die 'failed to create npg test db - test prerequisite';

  # edit the stored glob prepending the location of my test staging area
  my $folder_glob =  $schema_npg->resultset('Run')->find({id_run => $id_run, })->folder_path_glob;
  is($folder_glob, q[/{export,nfs}/sf39/ILorHSany_sf39/*/], 'folder glob retrieved OK - test prerequisite');
  $folder_glob = $test_dir . $folder_glob;
  lives_ok {$schema_npg->resultset('Run')->update_or_create({folder_path_glob => $folder_glob, id_run => $id_run, })} 'forder glob reset lives - test prerequisite';
  $folder_glob =  $schema_npg->resultset('Run')->find({id_run => $id_run, })->folder_path_glob;
  is($folder_glob, q[t/data/archive/{export,nfs}/sf39/ILorHSany_sf39/*/], 'new folder glob retrieved OK - test prerequisite');
  
  my $user_id = 1;
  lives_ok {$schema_npg->resultset('Run')->find({id_run => $id_run, })->set_tag($user_id, 'staging')} 'staging tag is set - test prerequisite';

  my $auto = $autoqc->retrieve($id_run, $schema_npg);

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

1;
