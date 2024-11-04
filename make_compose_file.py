num_nodes = 10  # Number of nodes you want to spin up

with open("docker-compose.yml", "w") as file:
    file.write("services:\n")

    for i in range(num_nodes):
        file.write(f"  node{i}:\n")
        file.write("    image: consensus\n")
        file.write("    network_mode: host\n")
        file.write("    environment:\n")
        file.write(f"      - NODE_ID={i}\n")
