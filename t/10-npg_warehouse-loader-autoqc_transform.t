use strict;
use warnings;
use Test::More tests => 2;
use Moose::Meta::Class;

use npg_qc::autoqc::results::genotype;

use_ok('npg_warehouse::loader::autoqc_transform');

my $transform = Moose::Meta::Class->create_anon_class(
  roles => [qw/npg_warehouse::loader::autoqc_transform/])->new_object({});

subtest 'Test genotype check transform' => sub {
  plan tests => 4;

  my $result_obj = npg_qc::autoqc::results::genotype->load(
    't/data/runfolders/110731_HS17_06624_A_B00T5ACXX/Data/Intensities/' .
    'BAM_basecalls_20110811-145428/no_cal/archive/lane3/plex1/qc/' .
    '6624_3#1.genotype.json'
  );

  my $data = $transform->genotype_transform($result_obj);
  is (keys(%$data), 3, 'three key-value pairs');
  is ($data->{'genotype_mean_depth'}, '50.12', 'genotype_mean_depth is correct');
  is ($data->{'genotype_sample_name_match'}, '23/25',
    'genotype_sample_name_match is correct');
  is ($data->{'genotype_sample_name_relaxed_match'}, '24/25',
    'genotype_sample_name_relaxed_match is correct');
};

1;
