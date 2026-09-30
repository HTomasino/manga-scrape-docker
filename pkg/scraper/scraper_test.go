package scraper

import (
	"testing"
)

func TestParseChapterNumber(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected float64
	}{
		{
			name:     "chapter with hyphen",
			input:    "chapter-5",
			expected: 5,
		},
		{
			name:     "chapter with digits",
			input:    "chapter10",
			expected: 10,
		},
		{
			name:     "chapter with hyphen and digits",
			input:    "chapter-137",
			expected: 137,
		},
		{
			name:     "chapter with space",
			input:    "chapter 5.5",
			expected: 5.5,
		},
		{
			name:     "chapter with underscore",
			input:    "chapter_123",
			expected: 123,
		},
		{
			name:     "chapter with equals",
			input:    "chapter=123",
			expected: 123,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ParseChapterNumber(tt.input)
			if result != tt.expected {
				t.Errorf("ParseChapterNumber(%q) = %v, want %v", tt.input, result, tt.expected)
			}
		})
	}
}
