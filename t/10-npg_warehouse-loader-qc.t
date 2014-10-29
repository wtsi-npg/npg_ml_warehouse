#########
# Author:        Marina Gourtovaia
# Maintainer:    $Author: mg8 $
# Created:       5 August 2010
# Last Modified: $Date: 2012-08-09 14:37:15 +0100 (Thu, 09 Aug 2012) $
# Id:            $Id: 10-npg_warehouse-loader-qc.t 15986 2012-08-09 13:37:15Z mg8 $
# $HeadURL: svn+ssh://svn.internal.sanger.ac.uk/repos/svn/new-pipeline-dev/data_handling/trunk/t/10-npg_warehouse-loader-qc.t $
#

use strict;
use warnings;
use Test::More tests => 20;
use Test::Exception;
use Test::Deep;

use t::npg_warehouse::util;

use_ok('npg_warehouse::loader::qc');

################################################################
#         Test cases description
################################################################
#batch_id # id_run # paired_id_run # paired_read # wh # npg # qc
################################################################
#2044     #  1272   # 1246          # 1           # 1  #  1  # 1
#4354     #  3500   # 3529          # 1           # 1  #  1  # 1
#4178     #  3323   # 3351          # 1           # 1  #  1  # 1
#4445     #  3622   #               # 0           # 1  #  1  # 1
#4915     #  3965   #               # 1           # 1  #  1  # 1
#4965     #  4025   #               # 1           # 1  #  1  # 1
#4380     #  3519   #               #             #    #  1  #
#5169     #  4138   #               #             #    #  1  #  this run is cancelled without qc complete status
#5498     #  4333   #               # 1           #    #  1  # 1 tag decoding stats added
          #  4779   # 
################################################################

my $util = t::npg_warehouse::util->new();
my $schema_qc;
my $index = 2;

{
  my $fixtures_path = q[t/data/fixtures/npgqc];
  lives_ok{ $schema_qc  = $util->create_test_db(q[npg_qc::Schema], $fixtures_path) } 'qc test db created';
}


{
  my $q;
  lives_ok {
       $q  = npg_warehouse::loader::qc->new( 
                                             schema_qc => $schema_qc, 
                                             reverse_end_index => $index,
                                             plex_key => 'plex'
                                           )
  } 'object instantiated by passing schema objects to the constructor';
  isa_ok ($q, 'npg_warehouse::loader::qc');
  is ($q->reverse_end_index, $index, 'reverse index set correctly');
  is ($q->verbose, 0, 'verbose mode is off by default');
}


{
  throws_ok {npg_warehouse::loader::qc->new(schema_qc => $schema_qc, plex_key => 'plex') } qr/Attribute \(reverse_end_index\) is required /,
    'error if reverse index is not set';
}


{
  my $q =  npg_warehouse::loader::qc->new( schema_qc => $schema_qc, 
                                           reverse_end_index => $index,
                                           plex_key => 'plex');
  throws_ok {$q->retrieve_cluster_density()} qr/Run id argument should be set/, 'error if id_run arg not set';
  throws_ok {$q->retrieve_summary()} qr/Run id argument should be set/, 'error if id_run arg not set';
  throws_ok {$q->retrieve_summary(22)} qr/End argument should be set/, 'error if end arg not set';
  throws_ok {$q->retrieve_summary(22,1)} qr/Two run folders flag argument should be set/, 'error if two run folders flag arg not set';
  throws_ok {$q->retrieve_yields()} qr/Run id argument should be set/, 'error if id_run arg not set';
}


{
  my $q =  npg_warehouse::loader::qc->new( schema_qc => $schema_qc, 
                                           reverse_end_index => $index,
                                           plex_key => 'plex');
  throws_ok {$q->retrieve_summary( 1, 2, 0)} qr/Reverse end index AND run with one runfolder/, 'error in get_run_qc_summary on a combination of a second read and one runfolder args';

  cmp_deeply($q->retrieve_summary( 22, 1, 1), {}, 'qc summary is an empty hash if run is not in the db');

  my $rows_ref = [{'clusters_pf' => '81851','lane_yield' => '363418','perc_pf_align_sd' => undef,'perc_pf_clusters_sd' => '2.40','clusters_pf_sd' => '4659','perc_pf_clusters' => '83.41','align_score' => undef,'perc_pf_align' => undef,'perc_error_rate' => undef,'first_cycle_int_sd' => '3','perc_error_rate_sd' => undef,'perc_int_20_cycles_sd' => '3.46','clusters_raw_sd' => '4765','lane' => '1','first_cycle_int' => '69','clusters_raw' => '98126','end' => '1','perc_int_20_cycles' => '75.60','align_score_sd' => undef},{'clusters_pf' => '95506','lane_yield' => '424046','perc_pf_align_sd' => undef,'perc_pf_clusters_sd' => '1.43','clusters_pf_sd' => '2161','perc_pf_clusters' => '84.83','align_score' => undef,'perc_pf_align' => undef,'perc_error_rate' => undef,'first_cycle_int_sd' => '1','perc_error_rate_sd' => undef,'perc_int_20_cycles_sd' => '0.97','clusters_raw_sd' => '2905','lane' => '2','first_cycle_int' => '45','clusters_raw' => '112604','end' => '1','perc_int_20_cycles' => '76.67','align_score_sd' => undef},{'clusters_pf' => '133450','lane_yield' => '592516','perc_pf_align_sd' => undef,'perc_pf_clusters_sd' => '1.64','clusters_pf_sd' => '2551','perc_pf_clusters' => '78.75','align_score' => undef,'perc_pf_align' => undef,'perc_error_rate' => undef,'first_cycle_int_sd' => '1','perc_error_rate_sd' => undef,'perc_int_20_cycles_sd' => '2.69','clusters_raw_sd' => '3975','lane' => '3','first_cycle_int' => '49','clusters_raw' => '169519','end' => '1','perc_int_20_cycles' => '74.23','align_score_sd' => undef},{'clusters_pf' => '132313','lane_yield' => '587469','perc_pf_align_sd' => '0.04','perc_pf_clusters_sd' => '1.62','clusters_pf_sd' => '3790','perc_pf_clusters' => '83.17','align_score' => '178.07','perc_pf_align' => '99.13','perc_error_rate' => '0.22','first_cycle_int_sd' => '2','perc_error_rate_sd' => '0.01','perc_int_20_cycles_sd' => '1.58','clusters_raw_sd' => '7318','lane' => '4','first_cycle_int' => '74','clusters_raw' => '159216','end' => '1','perc_int_20_cycles' => '79.46','align_score_sd' => '0.12'},{'clusters_pf' => '125077','lane_yield' => '555341','perc_pf_align_sd' => undef,'perc_pf_clusters_sd' => '0.93','clusters_pf_sd' => '1939','perc_pf_clusters' => '82.55','align_score' => undef,'perc_pf_align' => undef,'perc_error_rate' => undef,'first_cycle_int_sd' => '1','perc_error_rate_sd' => undef,'perc_int_20_cycles_sd' => '1.03','clusters_raw_sd' => '3519','lane' => '5','first_cycle_int' => '46','clusters_raw' => '151558','end' => '1','perc_int_20_cycles' => '76.82','align_score_sd' => undef},{'clusters_pf' => '131445','lane_yield' => '583616','perc_pf_align_sd' => undef,'perc_pf_clusters_sd' => '1.65','clusters_pf_sd' => '4088','perc_pf_clusters' => '60.65','align_score' => undef,'perc_pf_align' => undef,'perc_error_rate' => undef,'first_cycle_int_sd' => '1','perc_error_rate_sd' => undef,'perc_int_20_cycles_sd' => '1.15','clusters_raw_sd' => '1848','lane' => '6','first_cycle_int' => '56','clusters_raw' => '216692','end' => '1','perc_int_20_cycles' => '65.04','align_score_sd' => undef},{'clusters_pf' => '102878','lane_yield' => '456778','perc_pf_align_sd' => undef,'perc_pf_clusters_sd' => '1.96','clusters_pf_sd' => '5659','perc_pf_clusters' => '50.11','align_score' => undef,'perc_pf_align' => undef,'perc_error_rate' => undef,'first_cycle_int_sd' => '2','perc_error_rate_sd' => undef,'perc_int_20_cycles_sd' => '1.46','clusters_raw_sd' => '3720','lane' => '7','first_cycle_int' => '58','clusters_raw' => '205196','end' => '1','perc_int_20_cycles' => '64.96','align_score_sd' => undef},{'clusters_pf' => '49202','lane_yield' => '218459','perc_pf_align_sd' => undef,'perc_pf_clusters_sd' => '1.58','clusters_pf_sd' => '4114','perc_pf_clusters' => '89.22','align_score' => undef,'perc_pf_align' => undef,'perc_error_rate' => undef,'first_cycle_int_sd' => '8','perc_error_rate_sd' => undef,'perc_int_20_cycles_sd' => '3.97','clusters_raw_sd' => '4518','lane' => '8','first_cycle_int' => '58','clusters_raw' => '55147','end' => '1','perc_int_20_cycles' => '73.03','align_score_sd' => undef}];

  my $h = $q->retrieve_summary( 3622, 1, 1);
  my @a;
  foreach my $key (sort keys %{$h}) {
    push @a, $h->{$key}->{1};
  }
  cmp_deeply( \@a, $rows_ref, 'qc summary for run 3622 end 1');
}


{
  my $q =  npg_warehouse::loader::qc->new( schema_qc => $schema_qc, 
                                         reverse_end_index => $index,
                                         plex_key => 'plex');
  is (scalar keys %{$q->retrieve_cluster_density(3323)}, 0, 'no cluster densities for run 3622');  
}


{
  my $expected = {
                     1 => {'raw_cluster_density' => 95465.880,  'pf_cluster_density' => 11496.220,},
                     2 => {'raw_cluster_density' => 325143.800, 'pf_cluster_density' => 82325.490,},
                     3 => {'raw_cluster_density' => 335626.700, 'pf_cluster_density' => 171361.900,},
                     4 => {'raw_cluster_density' => 175608.400, 'pf_cluster_density' => 161077.600,},
                     5 => {'raw_cluster_density' => 443386.900, 'pf_cluster_density' => 380473.100,},
                     6 => {'raw_cluster_density' => 454826.200, 'pf_cluster_density' => 397424.100,},
                     7 => {'raw_cluster_density' => 611192.000, 'pf_cluster_density' => 465809.300,},
                     8 => {'raw_cluster_density' => 511924.700, 'pf_cluster_density' => 377133.300,},
                 };

  my $q =  npg_warehouse::loader::qc->new( schema_qc => $schema_qc, 
                                           reverse_end_index => $index,
                                           plex_key   => 'plex');
  my $values = $q->retrieve_cluster_density(4333);
  cmp_deeply ($q->retrieve_cluster_density(4333), $expected, 'cluster densities for run 4333');
}

{
  my $expected = {
          '1' => {
                   'q30_yield_kb_reverse_read' => '105906',
                   'q30_yield_kb_forward_read' => '98073',
                   'q40_yield_kb_forward_read' => '0'
                 },
          '2' => {
                   'q30_yield_kb_reverse_read' => '1003112',
                   'q30_yield_kb_forward_read' => '563558'
                 },
          '3' => {
                   'q30_yield_kb_reverse_read' => '1011728',
                   'q30_yield_kb_forward_read' => '981688'
                 },
          '4' => {
                   'q30_yield_kb_reverse_read' => '714510',
                   'q30_yield_kb_forward_read' => '745267',
                   'q40_yield_kb_forward_read' => '56',
                   'q40_yield_kb_reverse_read' => '37',
                 },
          '5' => {
                   'q30_yield_kb_reverse_read' => '1523282',
                   'q30_yield_kb_forward_read' => '1670331'
                 },
          '6' => {
                   'q30_yield_kb_reverse_read' => '1530965',
                   'q30_yield_kb_forward_read' => '1689674'
                 },
          '7' => {
                   'q30_yield_kb_reverse_read' => '997068',
                   'q30_yield_kb_forward_read' => '1668517'
                 },
          '8' => {
                   'q30_yield_kb_forward_read' => '1111015'
                 },
        };
  my $q =  npg_warehouse::loader::qc->new( schema_qc => $schema_qc, 
                                           reverse_end_index => $index,
                                           plex_key => 'plex');
  my $values = $q->retrieve_yields(4333);
  cmp_deeply ($values, $expected, 'q30 and q40 yields for run 4333 - all lanes not pools');
  ok(!exists $values->{1}->{plex}, 'no plexes retrieved');

  $expected =    {
 
          '2' => {
                   'q30_yield_kb_reverse_read' => '9820023',
                   'q30_yield_kb_forward_read' => '10236374',
                   'q40_yield_kb_forward_read' => '6887095',
                   'q40_yield_kb_reverse_read' => '5633534',
                   'plex' => { '168' => {
                                'q30_yield_kb_reverse_read' => '304',
                                'q30_yield_kb_forward_read' => '326',
                                'q40_yield_kb_forward_read' => '210',
                                'q40_yield_kb_reverse_read' => '168',
                                        }
                             }
                 },
          '4' => {
                   'q30_yield_kb_reverse_read' => '11820778',
                   'q30_yield_kb_forward_read' => '12548670',
                   'q40_yield_kb_forward_read' => '8315876',
                   'q40_yield_kb_reverse_read' => '5937501',
                    'plex' => { '0' => {
                                'q30_yield_kb_reverse_read' => '99353',
                                'q30_yield_kb_forward_read' => '113296',
                                'q40_yield_kb_forward_read' => '72788',
                                'q40_yield_kb_reverse_read' => '48668',
                                        },
                                '2' => {
                                'q30_yield_kb_reverse_read' => '1526977',
                                'q30_yield_kb_forward_read' => '1603453',
                                'q40_yield_kb_forward_read' => '1074747',
                                'q40_yield_kb_reverse_read' => '780841',
                                        },
                                '8' => {
                                'q30_yield_kb_reverse_read' => '1610054',
                                'q30_yield_kb_forward_read' => '1700217',
                                'q40_yield_kb_forward_read' => '1129798',
                                'q40_yield_kb_reverse_read' => '811467',
                                        },
                              } 
                 },
        };

  $values = $q->retrieve_yields(6624);
  cmp_deeply ($values, $expected, 'q30 and q40 yields for two pooled lanes of run 6624');
}

1;
