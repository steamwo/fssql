package parquet

import (
	"errors"
	"fmt"
)

type PageType int32

const (
	PageData PageType = iota
	PageIndex
	PageDictionary
	PageDataV2
)

type DataPageHeader struct {
	NumValues               int32
	Encoding                Encoding
	DefinitionLevelEncoding Encoding
	RepetitionLevelEncoding Encoding
	Statistics              *Statistics
}

type DictionaryPageHeader struct {
	NumValues int32
	Encoding  Encoding
	IsSorted  bool
}

type DataPageHeaderV2 struct {
	NumValues                   int32
	NumNulls                    int32
	NumRows                     int32
	Encoding                    Encoding
	DefinitionLevelsByteLength int32
	RepetitionLevelsByteLength int32
	IsCompressed                bool
	Statistics                  *Statistics
}

type PageHeader struct {
	Type                 PageType
	UncompressedPageSize int32
	CompressedPageSize   int32
	CRC                  *int32
	Data                 *DataPageHeader
	Dictionary           *DictionaryPageHeader
	DataV2               *DataPageHeaderV2
}

func DecodePageHeader(b []byte) (PageHeader, int, error) {
	r := newCompactReader(b)
	h, err := r.pageHeader()
	if err != nil {
		return PageHeader{}, 0, err
	}
	return h, r.off, nil
}

func (r *compactReader) pageHeader() (PageHeader, error) {
	var h PageHeader
	var haveType, haveUncompressed, haveCompressed bool
	last := int16(0)
	for {
		f, nl, err := r.field(last)
		if err != nil {
			return h, err
		}
		last = nl
		if f.typ == ctStop {
			break
		}
		switch f.id {
		case 1:
			v, err := r.i32()
			if err != nil {
				return h, err
			}
			h.Type = PageType(v)
			haveType = true
		case 2:
			v, err := r.i32()
			if err != nil {
				return h, err
			}
			h.UncompressedPageSize = v
			haveUncompressed = true
		case 3:
			v, err := r.i32()
			if err != nil {
				return h, err
			}
			h.CompressedPageSize = v
			haveCompressed = true
		case 4:
			v, err := r.i32()
			if err != nil {
				return h, err
			}
			h.CRC = &v
		case 5:
			v, err := r.dataPageHeader()
			if err != nil {
				return h, err
			}
			h.Data = &v
		case 7:
			v, err := r.dictionaryPageHeader()
			if err != nil {
				return h, err
			}
			h.Dictionary = &v
		case 8:
			v, err := r.dataPageHeaderV2()
			if err != nil {
				return h, err
			}
			h.DataV2 = &v
		default:
			if err := r.skip(f.typ, 1); err != nil {
				return h, err
			}
		}
	}
	if !haveType || !haveUncompressed || !haveCompressed {
		return h, errors.New("parquet: incomplete page header")
	}
	if h.UncompressedPageSize < 0 || h.CompressedPageSize < 0 {
		return h, fmt.Errorf("parquet: negative page size")
	}
	switch h.Type {
	case PageData:
		if h.Data == nil {
			return h, errors.New("parquet: data page missing header")
		}
	case PageDictionary:
		if h.Dictionary == nil {
			return h, errors.New("parquet: dictionary page missing header")
		}
	case PageDataV2:
		if h.DataV2 == nil {
			return h, errors.New("parquet: data v2 page missing header")
		}
	}
	return h, nil
}

func (r *compactReader) dataPageHeader() (DataPageHeader, error) {
	var h DataPageHeader
	last := int16(0)
	for {
		f, nl, err := r.field(last)
		if err != nil {
			return h, err
		}
		last = nl
		if f.typ == ctStop {
			return h, nil
		}
		switch f.id {
		case 1:
			v, err := r.i32()
			if err != nil {
				return h, err
			}
			h.NumValues = v
		case 2:
			v, err := r.i32()
			if err != nil {
				return h, err
			}
			h.Encoding = Encoding(v)
		case 3:
			v, err := r.i32()
			if err != nil {
				return h, err
			}
			h.DefinitionLevelEncoding = Encoding(v)
		case 4:
			v, err := r.i32()
			if err != nil {
				return h, err
			}
			h.RepetitionLevelEncoding = Encoding(v)
		case 5:
			v, err := r.statistics()
			if err != nil {
				return h, err
			}
			h.Statistics = &v
		default:
			if err := r.skip(f.typ, 1); err != nil {
				return h, err
			}
		}
	}
}

func (r *compactReader) dictionaryPageHeader() (DictionaryPageHeader, error) {
	var h DictionaryPageHeader
	last := int16(0)
	for {
		f, nl, err := r.field(last)
		if err != nil {
			return h, err
		}
		last = nl
		if f.typ == ctStop {
			return h, nil
		}
		switch f.id {
		case 1:
			v, err := r.i32()
			if err != nil {
				return h, err
			}
			h.NumValues = v
		case 2:
			v, err := r.i32()
			if err != nil {
				return h, err
			}
			h.Encoding = Encoding(v)
		case 3:
			v, err := r.boolValue(f)
			if err != nil {
				return h, err
			}
			h.IsSorted = v
		default:
			if err := r.skip(f.typ, 1); err != nil {
				return h, err
			}
		}
	}
}

func (r *compactReader) dataPageHeaderV2() (DataPageHeaderV2, error) {
	h := DataPageHeaderV2{IsCompressed: true}
	last := int16(0)
	for {
		f, nl, err := r.field(last)
		if err != nil {
			return h, err
		}
		last = nl
		if f.typ == ctStop {
			return h, nil
		}
		switch f.id {
		case 1:
			v, err := r.i32()
			if err != nil {
				return h, err
			}
			h.NumValues = v
		case 2:
			v, err := r.i32()
			if err != nil {
				return h, err
			}
			h.NumNulls = v
		case 3:
			v, err := r.i32()
			if err != nil {
				return h, err
			}
			h.NumRows = v
		case 4:
			v, err := r.i32()
			if err != nil {
				return h, err
			}
			h.Encoding = Encoding(v)
		case 5:
			v, err := r.i32()
			if err != nil {
				return h, err
			}
			h.DefinitionLevelsByteLength = v
		case 6:
			v, err := r.i32()
			if err != nil {
				return h, err
			}
			h.RepetitionLevelsByteLength = v
		case 7:
			v, err := r.boolValue(f)
			if err != nil {
				return h, err
			}
			h.IsCompressed = v
		case 8:
			v, err := r.statistics()
			if err != nil {
				return h, err
			}
			h.Statistics = &v
		default:
			if err := r.skip(f.typ, 1); err != nil {
				return h, err
			}
		}
	}
}
