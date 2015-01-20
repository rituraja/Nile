BEGIN { RS = "" ; FS = "\n" }
{
    print "line is ", $1
}