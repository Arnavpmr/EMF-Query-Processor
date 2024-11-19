import json
from helpers import tabs_to_spaces

class MFQueryProcessor:
    def __init__(self, inputs={}):
        self.inputs = inputs
    
    # Get the input from a json file and load data into inputs
    def get_input_from_file(self, filename):
        with open(f'inputs/{filename}.json', 'r') as f:
            self.inputs = json.load(f)
    
    # Get the input from stdin and load data into inputs
    def get_input_from_stdin(self):
        self.inputs["selections"] = parse_list_input(input("Enter the selections: "))
        self.inputs["n"] = int(input("Enter the value of n: "))
        self.inputs["grouping_attrs"] = parse_list_input(input("Enter the grouping attributes: "))
        self.inputs["aggregates"] = parse_list_input(input("Enter the aggregates: "))
        self.inputs["pred_list"] = parse_list_input(input("Enter the predicate list: "))
    
    # Returns the string for initializing the mf struct
    def initialize_mf_class(self):
        mf_class = f"""
class H:
    {"\n    ".join(list(map(lambda x: f"{x} = None", self.inputs["aggregates"])))}
    """
        return mf_class

    # Returns the string that populates the mf struct with the data from the query
    def populate_mf_struct(self):
        pass

    def getIthAggregates(self, i):
        return list(filter(lambda x: int(x.split("_")[1]) == i,self.inputs["aggregates"]))
    
    def generate_grouping_declaration(self):
        row_declarations = list(map(lambda x: f"row['{x}']", self.inputs["grouping_attrs"]))
        return f"{', '.join(self.inputs['grouping_attrs'])} = {", ".join(row_declarations)}"

    def __get_assignment_from_aggr(self, full_aggr, is_init):
        aggr, _, attr = full_aggr.split("_")

        if aggr == "count":
            return " = 1" if is_init else " += 1"
        elif aggr == "sum":
            return f" = row['{attr}']" if is_init else f" += row['{attr}']"
        elif aggr in ["max", "min"]:
            return f" = row['{attr}']" if is_init else f" = {aggr}(mf_struct[grouping_attrs_key].{full_aggr}, row['{attr}'])"

    def generate_aggr_assignments(self, aggrs, is_init):
        output = []

        for aggr in aggrs:
            output.append(f"mf_struct[grouping_attrs_key].{aggr}{self.__get_assignment_from_aggr(aggr, is_init)}")

        return f"{f"\n{tabs_to_spaces(4)}".join(output)}"
