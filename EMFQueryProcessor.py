import json, re
from helpers import parse_list_input, tts
from TopoSort import calculate_groupings_with_topo_sort

class EMFQueryProcessor:
    def __init__(self, inputs={}):
        self.inputs = inputs
    
    # Get the input from a json file and load data into inputs field
    def get_input_from_file(self, filename):
        with open(f'inputs/{filename}.json', 'r') as f:
            self.inputs = json.load(f)
        
        self.__preprocess_for_avg()
    
    # Get the input from stdin and load data into inputs field
    def get_input_from_stdin(self):
        self.inputs["selections"] = parse_list_input(input("Enter the selections: "))
        self.inputs["n"] = int(input("Enter the value of n: "))
        self.inputs["grouping_attrs"] = parse_list_input(input("Enter the grouping attributes: "))
        self.inputs["aggregates"] = parse_list_input(input("Enter the aggregates: "))
        self.inputs["pred_list"] = parse_list_input(input("Enter the predicate list: "))
        self.inputs["having_pred"] = input("Enter the having predicate: ")

        self.__preprocess_for_avg()
    
    # Returns the string for initializing each aggregate field in mf class
    def __get_mf_class_assignment_from_aggr(self, full_aggr):
        aggr = full_aggr.split("_")[0]
        if aggr == "max":
            return f"{full_aggr}=float('-inf')"
        elif aggr == "min":
            return f"{full_aggr}=float('inf')"
        else:
            return f"{full_aggr}=0"
    
    # Returns the string for initializing each grouping attribute in mf class
    def __get_mf_class_assignment_from_attr(self, attr):
        if attr in ["day", "month", "year", "quant"]:
            return "=0"
        else:
            return "=''"

    # Returns the string for initializing the mf class
    def initialize_mf_class(self):
        grouping_attr_assignments = list(
            map(
                lambda x: f"{x}{self.__get_mf_class_assignment_from_attr(x)}",
                self.inputs["grouping_attrs"]
            )
        )
        aggr_assignments = list(map(self.__get_mf_class_assignment_from_aggr, self.inputs["aggregates"]))
        selection_attr_assignments = list(
            map(
                lambda x: f"{x}{self.__get_mf_class_assignment_from_attr(x.split("_")[0])}",
                filter(
                    lambda x: re.match(r"^[a-z]+_\d+$", x),
                    self.inputs["selections"]
                )
            )
        )

        mf_class = (
             "class H:\n"
            f"\t{"\n\t"
               .join(
                   grouping_attr_assignments + selection_attr_assignments + aggr_assignments
                )
            }"
            "\n"
        )

        return mf_class

    # Returns a list of aggregates for the ith grouping variable
    def getIthAggregates(self, i):
        return list(filter(lambda x: int(x.split("_")[1]) == i, self.inputs["aggregates"]))
    
    # Returns the predicate for the ith grouping variable if there is one
    def getIthPredicate(self, i):
        predicates = list(filter(lambda x: int(x[0]) == i, self.inputs["pred_list"]))
        return predicates[0] if predicates else ""
    
    # Returns a list of selection attributes for the ith grouping variable
    def getIthSelectionAttrs(self, i):
        return list(
            filter(
                lambda x: re.match(f"^[a-z]+_{i}$", x),
                self.inputs["selections"]
            )
        )
    
    """
    This function scans the list of aggregates for any avg aggregates.
    For every avg aggregate, it adds the corresponding sum and count aggregates
    if they are not already present. It also moves all avg aggregates to the end
    to ensure they are calculated after the sum and count aggregates in the generated code.
    """
    def __preprocess_for_avg(self):
        non_avg_aggrs = set(list(filter(lambda x: x.split("_")[0] != "avg", self.inputs["aggregates"])))
        avg_aggrs = list(filter(lambda x: x.split("_")[0] == "avg", self.inputs["aggregates"]))

        for avg_aggr in avg_aggrs:
            _, i, attr = avg_aggr.split("_")
            if f"sum_{i}_{attr}" not in non_avg_aggrs:
                non_avg_aggrs.add(f"sum_{i}_{attr}")
            if f"count_{i}_{attr}" not in non_avg_aggrs:
                non_avg_aggrs.add(f"count_{i}_{attr}")
        
        self.inputs["aggregates"] = list(non_avg_aggrs) + avg_aggrs
    
    """
    Given a such that predicate, this function converts the predicate to a python expression.
    This python expression is then used in the generated code to check the predicate.
    """
    def __pred_to_py_exp(self, pred):
        main_attr_pattern = r"0\.([a-z]+)"
        var_attr_pattern = r"\d+\.([a-z]+)"
        aggr_pattern = r"([a-z]+_\d+_[a-z]+)"
        main_attr_replacement = r"h.\1"
        var_attr_replacement = r"row['\1']"

        pred = re.sub(main_attr_pattern, main_attr_replacement, pred)
        pred = re.sub(var_attr_pattern, var_attr_replacement, pred)
        return re.sub(aggr_pattern, r"h.\1", pred)
    
    """
    Given the where predicate, this function converts the predicate to a python expression.
    Although the where predicate is in the such that clause, a seperate function is needed since
    the 0th grouping variable condition needs to be processed differently.
    """
    def __get_where_pred_py_exp(self):
        pred = self.getIthPredicate(0)
        return re.sub(r"0\.([a-z]+)", r"row['\1']", pred)

    # Given the having predicate, this function converts the predicate to a python expression.
    def __get_having_pred_py_exp(self):
        temp = re.sub(r"([a-z]+_\d+_[a-z]+)", r"h.\1", self.inputs["having_pred"])
        return re.sub(r"0\.([a-z]+)", r"h.\1", temp)

    """
    Given an aggregate, this function returns the assignment to be made to the aggregate
    in order to update it in the table scan.
    """
    def __get_assignment_from_aggr(self, full_aggr):
        aggr, i, attr = full_aggr.split("_")

        if aggr == "count":
            return " += 1"
        elif aggr == "sum":
            return f" += row['{attr}']"
        elif aggr == "avg":
            return f" = h.sum_{i}_{attr}/h.count_{i}_{attr}"
        elif aggr in ["max", "min"]:
            return f" = {aggr}(h.{full_aggr}, row['{attr}'])"

    """
    Given a list of aggregates, this function generates the assignments to be made to
    them in order to update them in the table scan. It returns them as a single string and 
    automatically seperates them with a newline character to ensure they are on separate lines.
    This string can be directly inserted into the generated code that way.
    """
    def generate_aggr_assignments(self, aggrs, tab_count):
        return f"{tts(tab_count)}{f"\n{tts(tab_count)}".join(
            map(
                lambda x: f"h.{x}{self.__get_assignment_from_aggr(x)}",
                aggrs
            )
        )}\n"
    
    """
    Returns the assignments to be made to the grouping attributes in the table scan.
    The string is returned with a newline character separating each assignment to ensure
    that it can be directly inserted into the generated code that way.
    """
    def generate_grouping_attr_assignments(self, tabs):
        return f"{tts(tabs)}{f"\n{tts(tabs)}".join(
            map(
                lambda x: f"h.{x} = row['{x}']",
                self.inputs["grouping_attrs"]
            )
        )}\n"
    
    """
    This function returns the string which is a for loop that generates the output table.
    This output table is generated by iterating over the selections and adding them as cols
    to the output table.
    """
    def generate_output_loop(self):
        loop = "for h in mf_struct:\n"
        having_pred = self.__get_having_pred_py_exp()
        tabs = 2
        col_keys = ", ".join(map(lambda x: f"'{x}': h.{x}", self.inputs["selections"]))

        if having_pred:
            loop += f"{tts(tabs)}if {having_pred}:\n"
            tabs += 1
        
        loop += f"{tts(tabs)}output_table.append({{{col_keys}}})\n"

        return loop
    
    """
    This function generates the loop that initializes the mf_struct and updates
    the aggregates for the 0th grouping variable.
    """
    def generate_main_var_loop(self):
        loop = (
            "for row in cur:\n"
            f"{tts(2)}grouping_attrs_key = ({", ".join(list(map(lambda x: f"row['{x}']", self.inputs["grouping_attrs"])))})\n\n"
        )
        aggrs = self.getIthAggregates(0)
        predicate = self.__get_where_pred_py_exp()
        tabs = 2

        if predicate:
            loop += f"{tts(tabs)}if {predicate}:\n"
            tabs += 1
        
        loop += (
            f"{tts(tabs)}if grouping_attrs_key not in mf_struct_dict:\n"
            f"{tts(tabs+1)}h = H()\n"
        )

        loop += self.generate_grouping_attr_assignments(tabs+1)
        loop += (
            f"{tts(tabs+1)}mf_struct.append(h)\n"
            f"{tts(tabs+1)}mf_struct_dict[grouping_attrs_key] = h\n\n"
        )

        if aggrs:
            loop += f"{tts(tabs)}h = mf_struct_dict[grouping_attrs_key]\n"
            loop += self.generate_aggr_assignments(aggrs, tabs)

        return loop
    
    """
    Given a list of grouping variables as numbers, this function generates the loop
    that updates the aggregates and selection attributes for each grouping variable.
    """
    def __generate_multi_grouping_vars_loop(self, vars):
        loop = (
            "for row in cur:\n"
            f"{tts(2)}for h in mf_struct:\n"
        )
        init_tabs = 3
        where_pred = self.__get_where_pred_py_exp()

        if where_pred:
            loop += f"{tts(init_tabs)}if {where_pred}:\n"
            init_tabs += 1

        for var in vars:
            tabs = init_tabs
            aggrs = self.getIthAggregates(var)
            pred = self.getIthPredicate(var)
            selection_attrs = self.getIthSelectionAttrs(var)

            if pred:
                pred = self.__pred_to_py_exp(pred)
                loop += f"{tts(tabs)}if {pred}:\n"
                tabs += 1
            
            if selection_attrs:
                loop += f"{tts(tabs)}{f'\n{tts(tabs)}'.join(
                    map(
                        lambda x: f"h.{x} = row['{x.split("_")[0]}']", selection_attrs
                    )
                )}\n"

            if aggrs:
                loop += self.generate_aggr_assignments(aggrs, tabs)
        
        return loop
    
    """
    This function scans the predicate for each grouping variable and identifies all the aggregates
    used in the predicate. It uses this information to construct a graph of all the dependencies.
    """
    def __get_emf_dependency_graph(self):
        output = {}

        for i in range(1, self.inputs["n"] + 1):
            pred = self.getIthPredicate(i)

            if pred:
                dependencies = list(
                    filter(
                        lambda x: int(x.split("_")[1]) != 0,
                        re.findall(r"[a-z]+_\d+_[a-z]+", pred)
                    )
                )

                output[i] = list(
                    map(
                        lambda x: int(x.split("_")[1]),
                        dependencies
                    )
                )

        return output

    """
    This function generates the loops for each grouping variable in the order of their dependencies.
    It uses topological sort in order to determine the groups of grouping variables that can be updated
    in the same loop. It then generates the loop for each group of grouping variables and returns them as
    a string in the correct order. This ensures that the number of table scans is more minimized.
    """
    def generate_minimal_grouping_var_loops(self):
        dependency_graph = self.__get_emf_dependency_graph()

        # Order the groupings based on the dependencies from topo sort
        ordered_groupings = calculate_groupings_with_topo_sort(dependency_graph)

        return f"\n{tts(1)}cur.scroll(0, mode='absolute')\n\n{tts(1)}".join(
            list(
                map(
                    lambda x: self.__generate_multi_grouping_vars_loop(x),
                    ordered_groupings
                )
            )
        )