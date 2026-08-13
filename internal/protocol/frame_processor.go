package protocol

// implement processor to process frame
// processor start by send header -> method, body, ... body then dump to final type

type FrameProcessor struct {
	Offset uint8
	Frames []Frame
}

func NewFrameProcessor() FrameProcessor {
	return FrameProcessor{}
}

func (fp *FrameProcessor) Record(f Frame) {
	fp.Frames = append(fp.Frames, f)
}

func (fp *FrameProcessor) Reset() {
	fp.Offset = 0
	fp.Frames = fp.Frames[:0]
}

// build full frame
func (fp *FrameProcessor) Build() FullFrame {
	fullFrame := FullFrame{}
	return fullFrame
}
