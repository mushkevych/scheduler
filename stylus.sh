if ! hash stylus 2>/dev/null
then
    echo "'stylus' was not found in PATH"
    echo "Please install it via npm"
    echo "\t npm install -g stylus"
else
	stylus ./synergy/mx/static/styl/index.styl -o ./synergy/mx/static/css/index.css	
fi
