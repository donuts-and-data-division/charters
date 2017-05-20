import click
import yaml
import os
from os import path, system


@click.command()
@click.option('--yml', help='YAML-formatted database connection credentials.', required=True)
@click.option('--ddl', help='sql file.', required=True)
@click.option('--csv', help='CSV file to load into the database table', required=True)
def main(yml, ddl, csv):
    init(yml, ddl, csv)

def init(yml, ddl, csv):
    # We'll shell out to `psql`, so set the environment variables for it:
    #with open(yml) as f:
    #    for k,v in yaml.load(f).items():
    #        os.environ['PG' + k.upper()] = str(v) if v else ""

    # And create the table from the csv file with psql
    system("""psql -h pg.rcc.uchicago.edu -f {} capp30254_project1 -U capp30254_project1_user""".format(ddl))
    #system("""sudo service postgresql restart""")
    system("""psql -c "\copy {} FROM '{}' WITH CSV HEADER;" """.format("schools", csv))
    
    #system("""psql -c "alter table dedupe.entries add column entry_id SERIAL PRIMARY KEY;" """)
    #system("""psql -c "alter table dedupe.entries add column full_name VARCHAR;" """)
    #system("""psql -c "update dedupe.entries set full_name = first_name || ' ' || last_name;" """)

if __name__ == '__main__':
    main()

    #df.to_csv("test.csv")

   # system("""csvsql --db “postgresql://capp30254_project1_user:bokMatofAtt.@pg.rcc.uchicago.edu:5432/capp30254_project1”  --insert {} --overwrite""".format(csv))