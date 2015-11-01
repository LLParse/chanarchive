package fourchan

import "fmt"

type File struct {
	Md5   string `json:"md5"`
	Ext   string `json:"ext"`
	FSize int    `json:"fsize"`
	Data  []byte `json:"data"`

	Board string `json:"board"`
	Tim   int64  `json:"tim"`
}

type Post struct {
	Board          string      `json:"board,omitempty"`
	No             int         `json:"no"`
	Resto          int         `json:"resto,omitempty"`
	Sticky         uint8       `json:"sticky,omitempty"`
	Closed         uint8       `json:"closed,omitempty"`
	Archived       uint8       `json:"archived,omitempty"`
	Now            string      `json:"now,omitempty"`
	Time           int         `json:"time,omitempty"`
	Name           string      `json:"name,omitempty"`
	Trip           string      `json:"trip,omitempty"`
	Id             string      `json:"id,omitempty"`
	Capcode        string      `json:"capcode,omitempty"`
	Country        string      `json:"country,omitempty"`
	CountryName    string      `json:"country_name,omitempty"`
	Sub            string      `json:"sub,omitempty"`
	Com            string      `json:"com"`
	Tim            int64       `json:"tim"`
	Filename       string      `json:"filename"`
	Ext            string      `json:"ext"`
	FSize          int         `json:"fsize"`
	Md5            string      `json:"md5"`
	W              int         `json:"w,omitempty"`
	H              int         `json:"h,omitempty"`
	TnW            int         `json:"tn_w,omitempty"`
	TnH            int         `json:"tn_h,omitempty"`
	FileDeleted    uint8       `json:"filedeleted,omitempty"`
	Spoiler        uint8       `json:"spoiler,omitempty"`
	CustomSpoiler  int         `json:"custom_spoiler,omitempty"`
	OmittedPosts   int         `json:"omitted_posts,omitempty"`
	OmittedImages  int         `json:"omitted_images,omitempty"`
	Replies        int         `json:"replies,omitempty"`
	Images         int         `json:"images,omitempty"`
	BumpLimit      uint8       `json:"bumplimit,omitempty"`
	ImageLimit     uint8       `json:"imagelimit,omitempty"`
	CapcodeReplies interface{} `json:"capcode_replies,omitempty"`
	LastModified   int         `json:"last_modified,omitempty"`
	Op             bool        `json:"op,omitempty"`
}

func (p *Post) FSizePretty() string {
	b := p.FSize
	if b < 1024 {
		return fmt.Sprintf("%d B", b)
	}
	kb := b / 1024
	if kb < 1024 {
		return fmt.Sprintf("%d KB", kb)
	}
	mb := float64(kb) / 1024.
	return fmt.Sprintf("%.2f MB", mb)
}

type Thread struct {
	No    int     `json:"no"`
	Posts []*Post `json:"posts"`
}

type Board struct {
	Threads []Thread `json:"threads,omitempty"`
	Board   string   `json:"board"`
	Title   string   `json:"title"`
	WsBoard uint8    `json:"ws_board"`
	PerPage int      `json:"per_page"`
	Pages   int      `json:"pages"`
	LM      int
}

type ThreadInfo struct {
	Board        string `json:"board,omitempty"`
	No           int    `json:"no"`
	LastModified int    `json:"last_modified,omitempty"`
	MinPost      int    `json:"min_post,omitempty"`
	OwnerId      string `json:"owner_hint,omitempty"`
}

type ThreadPage struct {
	Board   string        `json:"board"`
	Page    int           `json:"page"`
	Threads []*ThreadInfo `json:"threads"`
}

type Boards struct {
	Boards []*Board `json:"boards"`
}
