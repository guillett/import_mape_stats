from sqlalchemy import create_engine, text
import os
import pandas as pd

# With pg_virtualenv
# pg_virtualenv zsh <<EOF
# createdb test
# python main.py
# EOF
connection_string = "postgresql://{}:{}@localhost/test".format(os.getenv("PGUSER"), os.getenv("PGPASSWORD"))

engine = create_engine(connection_string)

with engine.connect() as connection:
    result = connection.execute(text("select now()"))
    for row in result:
        print(row)

    df = pd.read_csv("Pages visitées Mes Aides 2022.csv", sep=";")

    section_data = df['Page - avec niveaux'].str.split('::', n=1, expand=True).rename(columns = lambda x: "section_"+str(x+1))
    full = df.merge(section_data, left_index=True, right_index=True)
    full.to_sql('tmp_mape_stats', connection)
    connection.commit()

with engine.connect() as connection:
    result = connection.execute(text("select * from tmp_mape_stats"))
    print(result.keys())
    for row in result:
        print(row)

