import subprocess
from MFQueryProcessor import MFQueryProcessor

#TODO
# Add support for avrg
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
    output_table = []

    {queryProcessor.generate_main_var_loop()}
    cur.scroll(0, mode='absolute')

    {queryProcessor.generate_grouping_vars_loop()}
    {queryProcessor.generate_output_loop()}
    return tabulate.tabulate(output_table,
                        headers="keys", tablefmt="psql")

def main():
    print(query())
    
if "__main__" == __name__:
    main()
    """

    output_path = f"outputs/{filename}_gen.py"
    # Write the generated code to a file
    open(output_path, "w").write(output_str)
    # Execute the generated code
    # subprocess.run(["python", output_path])


if "__main__" == __name__:
    main()
