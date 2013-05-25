=head1 NAME

PGObject - Base class for PG Object subclasses

=cut

package PGObject;
use strict;
use warnings;
use Carp;

=head1 VERSION

Version 1.0

=cut

our $VERSION = 1.0000;

=head1 SYNPOSIS


To get basic info from a function

  my $f_info = PGObject->function_info(
      dbh        =>  $dbh,
      funcname   =>  $funcname,
      funcschema =>  'public',
  );

To get info about a function, filtered by first argument type

  my $f_info = PGObject->function_info(
      dbh        =>  $dbh,
      funcname   =>  $funcname,
      funcschema =>  'public',
      objtype    =>  'invoice',
      objschema  =>  'public',
  );

To call a function with enumerated arguments

  my @results = PGObject->call_procedure(
      dbh          =>  $dbh,
      funcname     => $funcname,
      funcschema   => $funcname,
      args         => [$arg1, $arg2, $arg3],
  );

To do the same with a running total

  my @results = PGObject->call_procedure(
      dbh           =>  $dbh,
      funcname      => $funcname,
      funcschema    => $funcname,
      args          => [$arg1, $arg2, $arg3],
      running_funcs => [{agg => 'sum(amount)', alias => 'running_total'}],
  );

=head1 DESCRIPTION

PGObject contains the base routines for object management using discoverable
stored procedures in PostgreSQL databases.  This module contains only common
functionality and support structures, and low-level API's.  Most developers will
want to use more functional modules which add to these functions.

The overall approach here is to provide the basics for a toolkit that other 
modules can extend.  This is thus intended to be a component for building 
integration between PostgreSQL user defined functions and Perl objects.  

Because decisions such as state handling are largely outside of the scope of 
this module, this module itself does not do any significant state handling.  
Database handles (using DBD::Pg 2.0 or later) must be passed in on every call. 
This decision was made in order to allow for diversity in this area, with the 
idea that wrapper classes would be written to implement this.

=head1 FUNCTIONS

=over 

=item function_info(%args)

Arguments:

=over

=item dbh (required)

Database handle

=item funcname (required)

function name

=item funcschema (optional, default 'public')

function schema 

=item argtype1 (optional)

Name of first argument type.  If not provided, does not filter on this criteria.

=item argschema (optional)

Name of first argument type's schema.  If not provided defaults to 'public'

=back

This function looks up basic mapping information for a function.  If more than 
one function is found, an exception is raised.  This function is primarily 
intended to be used by packages which extend this one, in order to accomplish
stored procedure to object mapping.

Return data is a hashref containing the following elements:

=over

=item args

This is an arrayref of hashrefs, each of which contains 'name' and 'type'

=item name 

The name of the function

=item num_args

The number of arguments

=back

=cut

sub function_info {
    my ($self) = shift @_;
    my %args = @_;
    $args{funcschema} ||= 'public';
    $args{argschema} ||= 'public';

    my $dbh = $args{dbh};

    

    my $query = qq|
        SELECT proname, pronargs, proargnames, 
               string_to_array(array_to_string(proargtypes::regtype[], ' '), 
                               ' ') as argtypes
          FROM pg_proc 
          JOIN pg_namespace pgn ON pgn.oid = pronamespace
         WHERE proname = ? AND nspname = ?
    |;
    my @queryargs = ($args{funcname}, $args{funcschema});
    if ($args{argtype1}) {
       $query .= qq|
               AND (proargtypes::int[])[0] IN (select t.oid 
                                                 from pg_type t
                                                 join pg_namespace n
                                                      ON n.oid = typnamespace
                                                where typname = ? 
                                                      AND n.nspname = ?
       )|;
       push @queryargs, $args{argtype1};
       push @queryargs, $args{argschema};
    }

    my $sth = $dbh->prepare($query) || die $!;
    $sth->execute(@queryargs);
    my $ref = $sth->fetchrow_hashref('NAME_lc');
    croak "No such function" if !$ref;
    croak 'Ambiguous discovery criteria' if $sth->fetchrow_hashref('NAME_lc');

    my $f_args;
    for my $n (@{$ref->{proargnames}}){
        push @$f_args, {name => $n, type => shift @{$ref->{argtypes}}};
    }

    return {
        name     => $ref->{proname}, 
        num_args => $ref->{pronargs},
        args     => $f_args,
    };
    
}

=item call_procedure(%args)

Arguments:

=over

=item funcname

The function name

=item funcschema

The schema in which the function resides

=item args

This is an arrayref.  Each item is either a literal value, an arrayref, or a 
hashref of extended information.  In the hashref case, the type key specifies 
the string to use to cast the type in, and value is the value.

=item orderby

The list (arrayref) of columns on output for ordering.

=item running_funcs

An arrayref of running windowed aggregates.  Each contains two keys, namely 'agg' for the aggregate and 'alias' for the function name.

These are aggregates, each one has appended 'OVER (ROWS UNBOUNDED PRECEDING)' 
to it.  

Please note, these aggregates are not intended to be user-supplied.  Please only
allow whitelisted values here or construct in a tested framework elsewhere.
Because of the syntax here, there is no sql injection prevention possible at
the framework level for this parameter.

=back

=cut

sub call_procedure {
    my ($self) = shift @_;
    my %args = @_;
    $args{funcschema} ||= 'public';
    my $dbh = $args{dbh};

    my $wf_string = '';

    if ($args{running_funcs}){
        for (@{$args{running_funcs}}){
           $wf_string .= ', '. $_->{agg}. ' OVER (ROWS UNBOUNDED PRECEDING) AS '
                         . $_->{alias};
        }
    }
    my @qargs = ();
    my $argstr = '';
    for my $in_arg (@{$args{args}}){
        my $arg = $in_arg;
        if (eval {$in_arg->can('pgobject_to_db')}) {
            $arg = $in_arg->{pgobject_to_db};
        } 
            
        if ($argstr){
           $argstr .= ', ?';
        } else {
           $argstr .= '?';
        }
        if (ref $arg eq ref {}){
           $argstr .= "::".$dbh->quote_identifier($arg->{cast}) if $arg->{cast};
           push @qargs, $arg->{value};
        }  else {
           push @qargs, $arg;
        }
    }
    my $order = '';
    if ($args{orderby}){
        for my $ord (@{$args{orderby}}){
            my @words = split / /, $ord;
            my $direction = pop @words;
            my $safe_ord;

            if (uc($direction) =~ /^(ASC|DESC)$/){
              $ord =~ s/\s+$direction$//;
              $safe_ord = $dbh->quote_identifier($ord) . " $direction"; 
            } else {
               $safe_ord = $dbh->quote_identifier($ord);
            }
 
            if ($order){
                $order .= ', ' . $safe_ord;
            } else {
                $order =  $safe_ord;
            }
        }
    }
    my $query = qq|
           SELECT * $wf_string 
             FROM | . $dbh->quote_identifier($args{funcschema}) . '.' . 
                      $dbh->quote_identifier($args{funcname}) . qq|($argstr) |;
    if ($order){ 
       $query .= qq|
         ORDER BY $order |;
    }

    my $sth = $dbh->prepare($query) || die $!;

    my $place = 1;

    # This is needed to support byteas, which rquire special escaping during
    # the binding process.  --Chris T

    foreach my $carg (@qargs){
        if (ref($carg) eq ref {}){
            $sth->bind_param($place, $carg->{value},
                       { pg_type => $carg->{type} });
        } else {
            $sth->bind_param($place, $carg);
        }
        ++$place;
    }

    $sth->execute();

    my @rows = ();
    while (my $row = $sth->fetchrow_hashref('NAME_lc')){
       push @rows, $row;
    }
    return @rows;      
}

=back

=head1 WRITING PGOBJECT-AWARE HELPER CLASSES

One of the powerful features of PGObject is the ability to declare methods in
types which can be dynamically detected and used to serialize data for query
purposes. Objects which contain a pgobject_to_db(), that method will be called
and the return value used in place of the object.  This can allow arbitrary
types to serialize themselves in arbitrary ways.

For example a date object could be set up with such a method which would export
a string in yyyy-mm-dd format.  An object could look up its own definition and
return something like :

   { cast => 'dbtypename', value => '("A","List","Of","Properties")'}

If a scalar is returned that is used as the serialized value.  If a hashref is
returned, it must follow the type format:

  type  => variable binding type,
  cast  => db cast type
  value => literal representation of type, as intelligible by DBD::Pg

=head1 WRITING TOP-HALF OBJECT FRAMEWORKS FOR PGOBJECT

PGObject is intended to be the database-facing side of a framework for objects.
The intended structure is for three tiers of logic:

=over

=item  Database facing, low-level API's

=item  Object management modules

=item  Application handlers with things like database connection management.

=back

By top half, we are referring to the second tier.  The third tier exists in the
client application.

The PGObject module provides only low-level API's in that first tier.  The job
of this module is to provide database function information to the upper level
modules.

We do not supply type information, If your top-level module needs this, please
check out https://code.google.com/p/typeutils/ which could then be used via our
function mapping APIs here.



=head1 AUTHOR

Chris Travers, C<< <chris.travers at gmail.com> >>

=head1 BUGS

Please report any bugs or feature requests to C<bug-pgobject at rt.cpan.org>, or through
the web interface at L<http://rt.cpan.org/NoAuth/ReportBug.html?Queue=PGObject>.  I will be notified, and then you'll
automatically be notified of progress on your bug as I make changes.

=head1 SUPPORT

You can find documentation for this module with the perldoc command.

    perldoc PGObject


You can also look for information at:

=over 

=item * RT: CPAN's request tracker (report bugs here)

L<http://rt.cpan.org/NoAuth/Bugs.html?Dist=PGObject>

=item * AnnoCPAN: Annotated CPAN documentation

L<http://annocpan.org/dist/PGObject>

=item * CPAN Ratings

L<http://cpanratings.perl.org/d/PGObject>

=item * Search CPAN

L<http://search.cpan.org/dist/PGObject/>

=back

=head1 ACKNOWLEDGEMENTS

This code has been loosely based on code written for the LedgerSMB open source 
accounting and ERP project.  While that software uses the GNU GPL v2 or later,
this is my own reimplementation, based on my original contributions to that 
project alone, and it differs in signficant ways.   This being said, without
LedgerSMB, this module wouldn't exist, and without the lessons learned there, 
and the great people who have helped make this possible, this framework would 
not be half of what it is today.


=head1 SEE ALSO

=over

=item PGObject::Simple - Simple mapping of object properties to stored proc args

=item PGObject::Simple::Moose - Moose-enabled wrapper for PGObject::Simple

=back

=head1 COPYRIGHT

COPYRIGHT (C) 2013 Chris Travers

Redistribution and use in source and compiled forms with or without 
modification, are permitted provided that the following conditions are met:

=over

=item 

Redistributions of source code must retain the above
copyright notice, this list of conditions and the following disclaimer as the
first lines of this file unmodified.

=item 

Redistributions in compiled form must reproduce the above copyright
notice, this list of conditions and the following disclaimer in the
source code, documentation, and/or other materials provided with the 
distribution.

=back

THIS SOFTWARE IS PROVIDED BY THE AUTHOR(S) "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE AUTHOR(S) BE LIABLE FOR
ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

=cut

1;
