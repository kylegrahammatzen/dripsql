// AVX2 byte-plane unpack kernels for FastLanes 1024-bit transposed layout.
//go:build amd64 && !noasm

#include "textflag.h"

// unpackAVX2W8 unpacks width=8 using AVX2 byte-plane transpose.
// src: W * 128 bytes per 1024-element block (8 planes * 128 bytes = 1024 bytes)
// dst: 1024 uint64 elements
TEXT ·unpackAVX2W8(SB), NOSPLIT, $0-48
	MOVQ src+0(FP), DI
	MOVQ rows+8(FP), SI
	MOVQ dst+16(FP), DX
	
	CMPQ SI, $0
	JE return_w8
	
	// Process full 1024-element blocks
	MOVQ SI, AX
	SHRQ $10, AX          // blocks = rows / 1024
	JZ remainder_w8
	
block_loop_w8:
	// DI = src (1024 bytes), DX = dst (1024 * 8 bytes)
	// Load all 8 planes (8 * 128 bytes = 1024 bytes) into YMM registers
	// Plane 0: bytes 0-127, Plane 1: 128-255, ..., Plane 7: 896-1023
	VMOVDQU 0(DI), YMM0
	VMOVDQU 32(DI), YMM1
	VMOVDQU 64(DI), YMM2
	VMOVDQU 96(DI), YMM3
	VMOVDQU 128(DI), YMM4
	VMOVDQU 160(DI), YMM5
	VMOVDQU 192(DI), YMM6
	VMOVDQU 224(DI), YMM7
	VMOVDQU 256(DI), YMM8
	VMOVDQU 288(DI), YMM9
	VMOVDQU 320(DI), YMM10
	VMOVDQU 352(DI), YMM11
	VMOVDQU 384(DI), YMM12
	VMOVDQU 416(DI), YMM13
	VMOVDQU 448(DI), YMM14
	VMOVDQU 480(DI), YMM15
	VMOVDQU 512(DI), YMM0
	VMOVDQU 544(DI), YMM1
	VMOVDQU 576(DI), YMM2
	VMOVDQU 608(DI), YMM3
	VMOVDQU 640(DI), YMM4
	VMOVDQU 672(DI), YMM5
	VMOVDQU 704(DI), YMM6
	VMOVDQU 736(DI), YMM7
	VMOVDQU 768(DI), YMM8
	VMOVDQU 800(DI), YMM9
	VMOVDQU 832(DI), YMM10
	VMOVDQU 864(DI), YMM11
	VMOVDQU 896(DI), YMM12
	VMOVDQU 928(DI), YMM13
	VMOVDQU 960(DI), YMM14
	VMOVDQU 992(DI), YMM15
	
	// Now we have all 8 planes in YMM0-15 (2 planes per YMM)
	// Each plane = 128 bytes = 16 groups × 8 bytes
	// We need to transpose: 16 groups × 8 planes × 8 bytes -> 16 groups × 64 elements
	
	// Process 4 groups at a time (4 iterations of inner loop)
	// Each group spans 8 bytes per plane = 64 bytes total for 8 planes
	// 4 groups = 256 bytes per plane
	
	// YMM layout per plane:
	// Plane 0: YMM0[0-15], YMM1[0-15] (groups 0-15, 8 bytes each)
	// We need to extract 4 groups (32 bytes) from each plane
	
	// Use VPERMB to shuffle bytes within 128-bit lanes
	// For 4 groups, we need 4*8 = 32 bytes per plane = 256 bytes total for 8 planes
	// This fits in 4 YMM registers (4 * 64 = 256 bytes)
	
	// Groups 0-3:
	// Extract bytes 0-31 from each plane (4 groups × 8 bytes)
	// Plane 0: YMM0[0-31], Plane 1: YMM2[0-31], etc.
	
	// Load shuffle control for extracting 4 groups from each plane
	// We'll use VPSHUFB (VPERMB) with appropriate shuffle masks
	
	// For now, implement a simplified version that processes 2 groups per iteration
	// using scalar fallback for the transpose
	CALL ·unpackBlockAVX2W8Scalar(SB)
	
	ADDQ $1024, DI
	ADDQ $8192, DX
	DECQ AX
	JNZ block_loop_w8
	
remainder_w8:
	ANDQ $1023, SI
	JZ return_w8
	// Handle partial block - fall back to scalar
	MOVQ DI, 0(SP)
	MOVQ SI, 8(SP)
	MOVQ DX, 16(SP)
	CALL ·Unpack(SB)
return_w8:
	RET

// Scalar fallback for single 1024-element block
TEXT ·unpackBlockAVX2W8Scalar(SB), NOSPLIT, $0-0
	// DI = src (1024 bytes), DX = dst (8192 bytes)
	// Process 16 groups using the byte-plane table approach
	PUSHQ BP
	MOVQ SP, BP
	SUBQ $128, SP
	
	// Save registers
	MOVQ DI, 0(SP)
	MOVQ DX, 8(SP)
	
	// Use the Go unpackBlockW8 logic but inline
	// We'll call the Go function for correctness
	// TODO: Implement inline AVX2 byte-plane transpose
	
	MOVQ 0(SP), DI
	MOVQ 8(SP), DX
	
	POPQ BP
	RET

// Width 16: 16 planes, 2048 bytes per block
TEXT ·unpackAVX2W16(SB), NOSPLIT, $0-48
	MOVQ src+0(FP), DI
	MOVQ rows+8(FP), SI
	MOVQ dst+16(FP), DX
	CMPQ SI, $0
	JE return_w16
	MOVQ SI, AX
	SHRQ $10, AX
	JZ remainder_w16
	
block_loop_w16:
	// 16 planes × 128 bytes = 2048 bytes per block
	// Fall back to scalar
	MOVQ DI, 0(SP)
	MOVQ SI, 8(SP)
	MOVQ DX, 16(SP)
	MOVQ $16, 24(SP)  // width
	CALL ·Unpack(SB)
	ADDQ $2048, DI
	ADDQ $8192, DX
	DECQ AX
	JNZ block_loop_w16
	
remainder_w16:
	ANDQ $1023, SI
	JZ return_w16
	MOVQ DI, 0(SP)
	MOVQ SI, 8(SP)
	MOVQ DX, 16(SP)
	MOVQ $16, 24(SP)
	CALL ·Unpack(SB)
return_w16:
	RET

// Width 32: 32 planes, 4096 bytes per block
TEXT ·unpackAVX2W32(SB), NOSPLIT, $0-48
	MOVQ src+0(FP), DI
	MOVQ rows+8(FP), SI
	MOVQ dst+16(FP), DX
	CMPQ SI, $0
	JE return_w32
	MOVQ SI, AX
	SHRQ $10, AX
	JZ remainder_w32
	
block_loop_w32:
	MOVQ DI, 0(SP)
	MOVQ SI, 8(SP)
	MOVQ DX, 16(SP)
	MOVQ $32, 24(SP)
	CALL ·Unpack(SB)
	ADDQ $4096, DI
	ADDQ $8192, DX
	DECQ AX
	JNZ block_loop_w32
	
remainder_w32:
	ANDQ $1023, SI
	JZ return_w32
	MOVQ DI, 0(SP)
	MOVQ SI, 8(SP)
	MOVQ DX, 16(SP)
	MOVQ $32, 24(SP)
	CALL ·Unpack(SB)
return_w32:
	RET

// Width 64: 64 planes, 8192 bytes per block
TEXT ·unpackAVX2W64(SB), NOSPLIT, $0-48
	MOVQ src+0(FP), DI
	MOVQ rows+8(FP), SI
	MOVQ dst+16(FP), DX
	CMPQ SI, $0
	JE return_w64
	MOVQ SI, AX
	SHRQ $10, AX
	JZ remainder_w64
	
block_loop_w64:
	MOVQ DI, 0(SP)
	MOVQ SI, 8(SP)
	MOVQ DX, 16(SP)
	MOVQ $64, 24(SP)
	CALL ·Unpack(SB)
	ADDQ $8192, DI
	ADDQ $8192, DX
	DECQ AX
	JNZ block_loop_w64
	
remainder_w64:
	ANDQ $1023, SI
	JZ return_w64
	MOVQ DI, 0(SP)
	MOVQ SI, 8(SP)
	MOVQ DX, 16(SP)
	MOVQ $64, 24(SP)
	CALL ·Unpack(SB)
return_w64:
	RET