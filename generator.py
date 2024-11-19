import subprocess
from MFQueryProcessor import MFQueryProcessor


def main():
    """
    This is the generator code. It should take in the MF structure and generate the code
    needed to run the query. That generated code should be saved to a 
    file (e.g. _generated.py) and then run.
    """

    queryProcessor = MFQueryProcessor()
    input_choice = input("Enter 1 for file input and 2 for stdin input: ")

    if input_choice == "1":
        filename = input("Enter the filename without extension: ")
        queryProcessor.get_input_from_file(filename)
    else:
        queryProcessor.get_input_from_stdin()
    
    main_grouping_aggs = queryProcessor.getIthAggregates(0)
    main_grouping_aggs_loop = ""

    if (len(main_grouping_aggs) > 0):
        main_grouping_aggs_loop = f"""
    for row in cur:
        {queryProcessor.generate_grouping_declaration()}
        if grouping_attrs_key in mf_struct:
            {queryProcessor.generate_aggr_assignments(main_grouping_aggs, False)}
        else:
            mf_struct[grouping_attrs_key] = H()
            {queryProcessor.generate_aggr_assignments(main_grouping_aggs, True)}
    """

    grouping_var_aggs_loop = f"""
    for row in cur:
        for i in range(1, {queryProcessor.inputs["n"] + 1}):
    """

    body = """
    for row in cur:
        if row['quant'] > 10:
            _global.append(row)
    """

    # Note: The f allows formatting with variables.
    #       Also, note the indentation is preserved.
    output_str = f"""
import os
import psycopg2
import psycopg2.extras
import tabulate
from dotenv import load_dotenv

{queryProcessor.initialize_mf_class()}
def query():
    load_dotenv()

    user = os.getenv('USER')
    password = os.getenv('PASSWORD')
    dbname = os.getenv('DBNAME')

    conn = psycopg2.connect("dbname="+dbname+" user="+user+" password="+password,
                            cursor_factory=psycopg2.extras.DictCursor)
    cur = conn.cursor()
    cur.execute("SELECT * FROM sales")
    
    mf_struct = {{}}
    _global = []
    grouping_attrs_key = ({", ".join(queryProcessor.inputs["grouping_attrs"])})
    {main_grouping_aggs_loop}
    return tabulate.tabulate(_global,
                        headers="keys", tablefmt="psql")

def main():
    print(query())
    
if "__main__" == __name__:
    main()
    """

    # Write the generated code to a file
    open(f"outputs/{filename}_gen.py", "w").write(output_str)
    # Execute the generated code
    # subprocess.run(["python", "_generated.py"])


if "__main__" == __name__:
    main()
