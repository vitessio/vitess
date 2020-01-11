package visitorgen

// simplified ast
type (
	// SourceFile contains all important lines from an ast.go file
	SourceFile struct {
		lines []Sast
	}

	// Sast or simplified AST, is a representation of the ast.go lines we are interested in
	Sast interface {
		toSastString() string
	}

	// InterfaceDeclaration represents a declaration of an interface. This is used to keep track of which types
	// need to be handled by the visitor framework
	InterfaceDeclaration struct {
		name, block string
	}

	// TypeAlias is used whenever we see a `type XXX YYY` - XXX is the new name for YYY.
	// Note that YYY could be an array or a reference
	TypeAlias struct {
		name string
		typ  Type
	}

	// FuncDeclaration represents a function declaration. These are tracked to know which types implement interfaces.
	FuncDeclaration struct {
		receiver    *Field
		name, block string
		arguments   []*Field
	}

	// StructDeclaration represents a struct. It contains the fields and their types
	StructDeclaration struct {
		name   string
		fields []*Field
	}

	// Field is a field in a struct - a name with a type tuple
	Field struct {
		name string
		typ  Type
	}

	// Type represents a type in the golang type system. Used to keep track of type we need to handle,
	// and the types of fields.
	Type interface {
		toTypString() string
		rawTypeName() string
	}

	// TypeString is a raw type name, such as `string`
	TypeString struct {
		typName string
	}

	// Ref is a reference to something, such as `*string`
	Ref struct {
		inner Type
	}

	// Array is an array of things, such as `[]string`
	Array struct {
		inner Type
	}
)

var _ Sast = (*InterfaceDeclaration)(nil)
var _ Sast = (*StructDeclaration)(nil)
var _ Sast = (*FuncDeclaration)(nil)
var _ Sast = (*TypeAlias)(nil)

var _ Type = (*TypeString)(nil)
var _ Type = (*Ref)(nil)
var _ Type = (*Array)(nil)

// String returns a textual representation of the SourceFile. This is for testing purposed
func (t *SourceFile) String() string {
	var result string
	for _, l := range t.lines {
		result += l.toSastString()
		result += "\n"
	}

	return result
}

func (t *Ref) toTypString() string {
	return "*" + t.inner.toTypString()
}

func (t *Array) toTypString() string {
	return "[]" + t.inner.toTypString()
}

func (t *TypeString) toTypString() string {
	return t.typName
}

func (f *FuncDeclaration) toSastString() string {
	var receiver string
	if f.receiver != nil {
		receiver = "(" + f.receiver.String() + ") "
	}
	var args string
	for i, arg := range f.arguments {
		if i > 0 {
			args += ", "
		}
		args += arg.String()
	}

	return "func " + receiver + f.name + "(" + args + ") {" + blockInNewLines(f.block) + "}"
}

func (i *InterfaceDeclaration) toSastString() string {
	return "type " + i.name + " interface {" + blockInNewLines(i.block) + "}"
}

func (a *TypeAlias) toSastString() string {
	return "type " + a.name + " " + a.typ.toTypString()
}

func (s *StructDeclaration) toSastString() string {
	var block string
	for _, f := range s.fields {
		block += "\t" + f.String() + "\n"
	}

	return "type " + s.name + " struct {" + blockInNewLines(block) + "}"
}

func blockInNewLines(block string) string {
	if block == "" {
		return ""
	}
	return "\n" + block + "\n"
}

// String returns a string representation of a field
func (f *Field) String() string {
	if f.name != "" {
		return f.name + " " + f.typ.toTypString()
	}

	return f.typ.toTypString()
}

func (t *TypeString) rawTypeName() string {
	return t.typName
}

func (t *Ref) rawTypeName() string {
	return t.inner.rawTypeName()
}

func (t *Array) rawTypeName() string {
	return t.inner.rawTypeName()
}
