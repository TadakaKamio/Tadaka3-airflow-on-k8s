import inspect

def MyAppLog():
    # Get the caller's filename
    caller_frame = inspect.stack()[1]
    caller_file = caller_frame.filename
    
    print(f"MyAppLog was imported by: {caller_file}")
    print("Hello from MyAppLog function!")
