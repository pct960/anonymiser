package main

import (
	"github.com/lucasjones/reggen"
)

func main() {
	num, _ := reggen.Generate("^(7|8|9){1}[0-9]{9}$", 1)
	print(num)
}
