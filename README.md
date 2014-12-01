# MapReduceJS.Server

by Marc-André Bühler

A proof-of-concept for a simple map reduce infrastructure using web workers
on the client side and Node.js on the server side.

## Requirements

- Node.js
- npm
- tsc
- tsd (to be exact: tsd@next)

## Installation

### Client Installation

	cd ./MapReduceJS.Client
    npm install

### Server Installation

	cd ./MapReduceJS.Server
    npm install

## Run

Start the server with

	cd ./MapReduceJS.Server
    PORT=3000 node .

Now open a browser and navigate to http://localhost:3000/

Check the terminal you are running node from for workers connecting and requesting
jobs. Open the browser console for information about the spawned web workers.


## Development

I recommend to install the `nodemon` module globally using

    npm install -g nodemon

and then start the server with

    nodemon .


## License

See LICENSE file.