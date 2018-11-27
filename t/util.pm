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

1;
