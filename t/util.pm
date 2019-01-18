package t::util;

use strict;
use warnings;
use npg_tracking::glossary::composition::factory;
use npg_tracking::glossary::composition::component::illumina;

sub find_or_save_composition {
  my ($schema, @query) = @_;

  my $component_rs   = $schema->resultset('SeqComponent');
  my $composition_rs = $schema->resultset('SeqComposition');
  my $com_com_rs     = $schema->resultset('SeqComponentComposition');

  my $f = npg_tracking::glossary::composition::factory->new();
  my @components = ();

  for my $h (@query) {
    my @temp = %{$h};
    my %temp_hash = @temp;
    my $component_h = \%temp_hash;
    my $component = npg_tracking::glossary::composition::component::illumina->new($component_h);
    $f->add_component($component);
    $component_h->{'digest'} = $component->digest;
    push @components, $component_h;
  }
  
  my $composition = $f->create_composition();
  my $size = $composition->num_components;
  my $composition_digest = $composition->digest;
  my $composition_row = $composition_rs->find({digest => $composition_digest});

  if (!$composition_row) {
    $composition_row = $composition_rs->create(
      {size => $size, digest => $composition_digest});   
    for my $component_h (@components) {
      my $component_row = $component_rs->create($component_h);
      $com_com_rs->create(
        {size               => $size,
         id_seq_component   => $component_row->id_seq_component,
         id_seq_composition => $composition_row->id_seq_composition});
    }
  }

  return $composition_row->id_seq_composition;
}

sub create_nv_run {
  my ($schema_npg, $id_run, $folder_glob, $folder_name) = @_;

  $schema_npg->resultset('Run')->create({
    folder_path_glob => $folder_glob,
    id_run => $id_run,
    folder_name => $folder_name,
    is_paired => 1,
    id_instrument_format => 12,
    id_instrument => 90,
    team => '"joint"',
    actual_cycle_count => 318,
    expected_cycle_count => 318    
  });
  
  my $user_id = 7;
  my $run = $schema_npg->resultset('Run')->find({id_run => $id_run, });
  $run->set_tag($user_id, 'staging');
  $run->set_tag($user_id, 'workflow_NovaSeqXp');
  $run->set_tag($user_id, 'fc_slotA');

  for my $p ((1, 2)) {
    $schema_npg->resultset('RunLane')->create({
      id_run => $id_run, tile_count => 704, tracks => 1, position => $p});
  }

  return;
}

1;
