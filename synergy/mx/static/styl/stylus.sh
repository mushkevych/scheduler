if ! hash stylus 2>/dev/null
then
    echo "'stylus' was not found in PATH"
    echo "Please install it via npm"
    echo "\t npm install -g stylus"
else
	stylus ./index.styl -o ../css/index.css
fi
