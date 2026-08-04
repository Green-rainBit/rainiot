package logmulti

import (
	"fmt"
	"os"

	"github.com/zeromicro/go-zero/core/logx"
)

// ConsoleWriter 实现 logx.Writer，输出到控制台（带颜色）。
// ANSI 颜色仅 Windows 10+ / Linux / macOS 生效。
type ConsoleWriter struct{}

func NewConsoleWriter() *ConsoleWriter { return &ConsoleWriter{} }

func (c *ConsoleWriter) Alert(v any)                     { c.print("35", "ALERT", v) }
func (c *ConsoleWriter) Close() error                    { return nil }
func (c *ConsoleWriter) Severe(v any)                    { c.print("31;1", "SEVERE", v) }
func (c *ConsoleWriter) Error(v any, _ ...logx.LogField) { c.print("31", "ERROR", v) }
func (c *ConsoleWriter) Info(v any, _ ...logx.LogField)  { c.print("32", "INFO", v) }
func (c *ConsoleWriter) Slow(v any, _ ...logx.LogField)  { c.print("33", "SLOW", v) }
func (c *ConsoleWriter) Debug(v any, _ ...logx.LogField) { c.print("36", "DEBUG", v) }
func (c *ConsoleWriter) Stack(v any)                     { c.print("37", "STACK", v) }
func (c *ConsoleWriter) Stat(v any, _ ...logx.LogField)  { c.print("34", "STAT", v) }

func (c *ConsoleWriter) print(color, level string, v any) {
	fmt.Fprintf(os.Stdout, "\x1b[%sm[%s]\x1b[0m %v\n", color, level, v)
}
