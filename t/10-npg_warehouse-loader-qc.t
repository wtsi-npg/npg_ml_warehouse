use strict;
use warnings;
use Test::More tests => 14;
use Test::Exception;
use Moose::Meta::Class;
use npg_testing::db;

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

my $util = Moose::Meta::Class->create_anon_class(
  roles => [qw/npg_testing::db/])->new_object({});

my $schema_qc;
my $index = 2;

lives_ok{ $schema_qc  = $util->create_test_db(q[npg_qc::Schema],
  q[t/data/fixtures/npgqc]) } 'qc test db created';

{
  throws_ok {npg_warehouse::loader::qc->new(schema_qc => $schema_qc, plex_key => 'plex') }
    qr/Attribute \(reverse_end_index\) is required /,
    'error if reverse index is not set';

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
  my $q =  npg_warehouse::loader::qc->new( schema_qc => $schema_qc, 
                                           reverse_end_index => $index,
                                           plex_key => 'plex');
  throws_ok {$q->retrieve_cluster_density()} qr/Run id argument should be set/, 'error if id_run arg not set';
  throws_ok {$q->retrieve_yields()} qr/Run id argument should be set/, 'error if id_run arg not set';
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
  is_deeply ($q->retrieve_cluster_density(4333), $expected, 'cluster densities for run 4333');
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
  is_deeply ($values, $expected, 'q30 and q40 yields for run 4333 - all lanes not pools');
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
  is_deeply ($values, $expected, 'q30 and q40 yields for two pooled lanes of run 6624');
}

1;
