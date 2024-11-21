# Every input that is a list is in the form of item1; item2; item3;...
def parse_list_input(input: str) -> list[str]:
    return input.replace(" ", "").replace("\n", "").split(";")

# Returns a string with spaces for the given number of tabs
def tts(num_tabs: int) -> str:
    return "    " * num_tabs