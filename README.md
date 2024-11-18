# ॐ: A Tiny Document Search Engine

Welcome to ॐ! 🎉 ॐ is a lightweight document search engine designed to help you
index and search through your documents effortlessly. Built with
[Apache Tika](https://tika.apache.org/) for indexing and featuring multiple
pluggable backends for querying, ॐ currently supports
[Meilisearch](https://www.meilisearch.com/) and
[Sonic](https://github.com/valeriansaliou/sonic).

## Why ॐ?

ॐ is a personal project that allows me to experiment and iterate on ideas
quickly. It’s a fun way for me to explore the world of document search and
indexing, and I hope you find it interesting too!

If you're looking for a robust solution to search through a large collection of
unstructured data for production use, I recommend checking out
[Aleph](https://docs.aleph.occrp.org/) or
[Datashare](https://datashare.icij.org/). They are fantastic tools that are more
suited for production environments.

## Features

- **Document Indexing**: Leverage the power of
  [Apache Tika](https://tika.apache.org/) to extract text and metadata from
  various document formats.
- **Pluggable Backends**: Choose between different backends for querying your
  indexed documents. Currently supported backends include:
  - [Meilisearch](https://www.meilisearch.com/): A powerful search engine with
    AI features.
  - [Sonic](https://github.com/valeriansaliou/sonic): A lightweight search
    engine, an excellent choice for environments with limited resources.
- **Lightweight and Easy to Use**: ॐ is designed to be simple and
  straightforward, making it easy to get started with document search.

## Getting Started

### Prerequisites

Before you begin, ensure you have the following installed:

- [Poetry](https://python-poetry.org/) or [Nix](https://nixos.org/) for
  dependency management.
- [Apache Tika](https://tika.apache.org/) `tika-server`.
- [Meilisearch](https://www.meilisearch.com/) or
  [Sonic](https://github.com/valeriansaliou/sonic) (depending on your choice of
  backend).

### Installation

#### Using Poetry

```bash
# Clone the repository
git clone https://github.com/PapayaJackal/aum.git
cd aum
# Install the required dependencies and run the project
poetry install
poetry run aum
```

#### Using Nix

```bash
# Clone the repository
git clone https://github.com/PapayaJackal/aum.git
cd aum
# Use nix to build and run the project
nix build
./result/bin/aum
```

### Usage

ॐ can be used directly from the command line interface (CLI). Here’s how to get
started:

**Index Your Documents**: Use the CLI to index your documents. Specify the
directory containing your documents and the name of the index you want to use.

```bash
aum index index_name /path/to/your/documents
```

**Serve the Web Interface**: Once indexed, you can serve the web interface to
interact with your documents. ॐ provides a simple command to start the web
server.

```bash
aum serve index_name
```

After starting the server, you can access the web interface in your browser at
[http://localhost:8000](http://localhost:8080).

### Using docker-compose

If you prefer to run the project using Docker, you can use the provided
docker-compose.yml file. This method simplifies the setup process by
containerizing the application and its dependencies.

Clone the repository if you haven't already:

```bash
# Clone the repository
git clone https://github.com/PapayaJackal/aum.git
cd aum
```

Edit the docker-compose.yml file to mount your data directory. Open the
docker-compose.yml file in your preferred text editor and locate the service
definition for the application. You will need to add a volume mapping to mount
your local data directory to the container.

```yaml
volumes:
  - .:/app
  # Change this path to the directory where your data is stored
  - ./tests/data/:/data:ro
```

Build and run the containers using Docker Compose:

```bash
docker-compose up
```

Once the containers are up and running, you can access the web interface in your
browser at [http://localhost:8000](http://localhost:8000).

## Contributing

While ॐ is primarily a personal project, contributions are always welcome! If
you have ideas for improvements, bug fixes, or new features, feel free to open
an issue or submit a pull request.

### How to Contribute

1. Fork the repository.
1. Create a new branch for your feature or bug fix.
1. Make your changes and commit them.
1. Push your branch and open a pull request.

## License

This project is licensed under the **WTFPL** (Do What The Fuck You Want To
Public License). See the LICENSE file for more details.

## Acknowledgments

A big thank you to the developers of [Apache Tika](https://tika.apache.org/),
[Meilisearch](https://www.meilisearch.com/), and
[Sonic](https://github.com/valeriansaliou/sonic) for their amazing work that
made this project possible!

## Stay in Touch

If you have any questions, suggestions, or just want to chat about document
search engines, feel free to reach out on
[GitHub](https://github.com/PapayaJackal/aum/issues/new), or via email
[9e7uqyth@anonaddy.me](mailto:9e7uqyth@anonaddy.me).

Happy searching! 🚀
