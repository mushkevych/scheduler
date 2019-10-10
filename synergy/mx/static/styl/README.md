# How to compile CSS using Stylus

The stylesheets for the Scheduler UI are compiled using the CSS pre-processor, [Stylus](http://stylus-lang.com/).
Stylus files use the file-extension `*.styl`. In this project, `.styl` files are contained in `./synergy/mx/static/styl/`.
The `.styl` files are separated logically by category with `index.styl` containing all other `*.styl` files.  The syntax is similar to that of SASS or LESS, but traditional CSS syntax may be used as well.

## Installation

Install the `stylus` package with
```bash
npm install -g stylus
```

## Usage

To compile the Stylus files as CSS to `./synergy/mx/static/css/`:

### Using `stylus.sh` script

* To compile CSS:
    From the the `styl` directory, run
    ```bash
    ./stylus.sh
    ```
* To compile CSS **and watch for changes**:
    From the `styl` directory, run
    ```bash
    ./stylus-watch.sh
    ```

### Manually
* To compile CSS:
    From the **project root directory**, run
    ```bash
    stylus -w ./synergy/mx/static/styl/index.styl -o ./synergy/mx/static/css/index.css
    ```
* To compile CSS **and watch for changes**:
    From the **project root directory**, run
    ```bash
    stylus ./synergy/mx/static/styl/index.styl -o ./synergy/mx/static/css/index.css
    ```