import os, subprocess
from EMFQueryProcessor import EMFQueryProcessor

def main():
    """
    This project is an EMF Query Processor that takes in a JSON file or stdin input that represents the EMF query
    to process on the sales table. The program effectively generates another python program that can be executed in order
    to run the inputted query.
    """

    queryProcessor = EMFQueryProcessor()

    input_choice = input("Enter 1 for file input and 2 for stdin input: ")
    output_file = ""

    if input_choice == "1":
        filename = input("Enter the input json filename without extension: ")
        output_file = filename
        queryProcessor.get_input_from_file(filename)
    else:
        output_file = input("Enter the output py filename without extension: ")
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
    
    mf_struct = []
    mf_struct_dict = {{}}
    output_table = []

    {queryProcessor.generate_main_var_loop()}
    cur.scroll(0, mode='absolute')

    {queryProcessor.generate_minimal_grouping_var_loops()}
    {queryProcessor.generate_output_loop()}
    return tabulate.tabulate(output_table,
                        headers="keys", tablefmt="psql")

def main():
    print(query())
    
if "__main__" == __name__:
    main()
    """

    output_path = os.path.join("outputs", f"{output_file}.py")

    # Ensure the outputs directory exists
    os.makedirs("outputs", exist_ok=True)

    # Write the generated code to a file
    open(output_path, "w").write(output_str)

    # Execute the generated code
    # subprocess.run(["python", output_path])


if "__main__" == __name__:
    main()
