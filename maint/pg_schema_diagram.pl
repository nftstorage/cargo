#!/usr/bin/perl

use warnings;
use strict;

use DBIx::Class::Schema::Loader;
use DBIx::Class::_Util 'sigwarn_silencer';
use SQL::Translator;

{
  package GraphedSchema;
  use base 'DBIx::Class::Schema::Loader';

  __PACKAGE__->loader_options (
    naming => 'v8',
    db_schema => 'cargo',
  );
}

$SIG{__WARN__} = sigwarn_silencer(qr/collides with an inherited method/);

my $trans = SQL::Translator->new(
    parser        => 'SQL::Translator::Parser::DBIx::Class',
    parser_args   => { dbic_schema => GraphedSchema->connect('dbi:Pg:service=cargo', 'cargo') },
    producer      => 'GraphViz',
    producer_args => {
        width => 0,
        height => 0,
        output_type      => 'svg',
        out_file         => 'pg_schema_diagram.svg',
        show_constraints => 1,
        show_datatypes   => 1,
        show_indexes     => 1,
        show_sizes       => 1,
    },
) or die SQL::Translator->error;
$trans->translate or die $trans->error;
