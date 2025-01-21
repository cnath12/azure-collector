# check_env.py
def read_env_file():
    with open('.env', 'r') as f:
        print("Raw .env file contents:")
        print("-" * 50)
        print(f.read())
        print("-" * 50)

    # Now read line by line
    print("\nParsing .env file line by line:")
    with open('.env', 'r') as f:
        for line in f:
            if line.strip() and not line.startswith('#'):
                key, value = line.strip().split('=', 1)
                print(f"{key}: Length={len(value)}")

if __name__ == "__main__":
    read_env_file()